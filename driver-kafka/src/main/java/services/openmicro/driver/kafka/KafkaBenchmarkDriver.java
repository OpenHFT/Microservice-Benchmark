/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package services.openmicro.driver.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import services.openmicro.driver.api.Driver;
import services.openmicro.driver.api.Event;
import services.openmicro.driver.api.EventHandler;
import services.openmicro.driver.api.Producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class KafkaBenchmarkDriver implements Driver {

    static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    private int warmup;
    private short replicationFactor;
    private boolean reset;
    private Map<String, String> commonConfig, topicConfig, producerConfig, consumerConfig;
    private KafkaEvent event;
    private int producers = 1, partitions = 1, consumers = 1;

    private EventMicroservice microservice;
    private transient List<KafkaBenchmarkProducer> producerList = Collections.synchronizedList(new ArrayList<>());
    private transient List<Closeable> consumerList = Collections.synchronizedList(new ArrayList<>());
    private transient AdminClient admin;

    static KafkaEvent bytesToEvent(byte[] value) throws IOException {
        return jsonMapper.readValue(value, KafkaEvent.class);
    }

    public static byte[] eventToBytes(KafkaEvent event) throws JsonProcessingException {
        return jsonMapper.writeValueAsBytes(event);
    }

    @Override
    public void init() {
        Driver.super.init();

        Properties commonProperties = new Properties();
        commonProperties.putAll(commonConfig);

        producerConfig.putAll(commonConfig);

        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        consumerConfig.putAll(commonConfig);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        admin = AdminClient.create(commonProperties);

        if (reset) {
            // List existing topics
            ListTopicsResult result = admin.listTopics();
            try {
                Set<String> topics = result.names().get();
                // Delete all existing topics
                DeleteTopicsResult deletes = admin.deleteTopics(topics);
                deletes.all().get();
            } catch (InterruptedException | ExecutionException e) {
                LoggerFactory.getLogger(getClass()).warn("Error on reset", e);
                throw new IORuntimeException(e);
            }
        }
    }

    @Override
    public Producer createProducer(Consumer<Event> eventConsumer) {
        try {
            String id = Long.toString(System.currentTimeMillis(), 36);
            final String one = "one" + id;
            final String two = "two" + id;
            createTopics(one, two, partitions);

            final KafkaBenchmarkProducer producer2 = createProducerFor(two);


            final KafkaBenchmarkProducer producer = createProducerFor(one);

            for (int i = 0; i < consumers; i++) {
                this.microservice = new EventMicroservice(producer2);
                createConsumerFor(one, this.microservice);

                createConsumerFor(two, e -> {
                    synchronized (eventConsumer) {
                        eventConsumer.accept(e);
                    }
                });
            }

            return startTimeNS -> {
                event.sendingTimeNS(startTimeNS);
                event.transactTimeNS(0);
                producer.event(event);
            };

        } catch (ExecutionException | InterruptedException e) {
            throw new IORuntimeException(e);
        }
    }

    private void createConsumerFor(String topic, EventHandler<KafkaEvent> microservice) {
        Map<String, String> config = new HashMap<>(consumerConfig);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>((Map) config);
        consumer.subscribe(Arrays.asList(topic));
        final KafkaBenchmarkConsumer consumer1 = new KafkaBenchmarkConsumer(consumer, consumerConfig, microservice);
        consumerList.add(consumer1);
    }

    private KafkaBenchmarkProducer createProducerFor(String topic) {
        KafkaProducer<String, byte[]>[] kafkaProducers =
                IntStream.range(0, producers)
                        .mapToObj(i -> new KafkaProducer<>((Map<String, Object>)(Map) producerConfig))
                        .toArray(i -> (KafkaProducer<String, byte[]>[]) new KafkaProducer[i]);
        final KafkaBenchmarkProducer producer = new KafkaBenchmarkProducer(kafkaProducers, topic, partitions);
        producerList.add(producer);
        return producer;
    }

    private void createTopics(String one, String two, int partitions) throws ExecutionException, InterruptedException {
        final KafkaFuture<Void> oneFuture = createTopic(one, partitions);
        final KafkaFuture<Void> twoFuture = createTopic(two, partitions);
        oneFuture.get();
        twoFuture.get();
    }

    private KafkaFuture<Void> createTopic(String one, int partitions) {
        NewTopic newTopic = new NewTopic(one, partitions, replicationFactor);
        newTopic.configs(topicConfig);
        final KafkaFuture<Void> oneFuture = admin.createTopics(Arrays.asList(newTopic)).all();
        return oneFuture;
    }

    @Override
    public void start() {
        Driver.super.start();
    }

    @Override
    public void close() {
        for (KafkaBenchmarkProducer producer : producerList) {
            try {
                producer.close();
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).error("Error closing " + producer, e);
            }
        }

        for (Closeable consumer : consumerList) {
            try {
                consumer.close();
            } catch (IOException e) {
                LoggerFactory.getLogger(getClass()).error("Error closing " + consumer, e);
            }
        }
        admin.close();
    }

    @Override
    public int warmup() {
        return warmup;
    }
}
