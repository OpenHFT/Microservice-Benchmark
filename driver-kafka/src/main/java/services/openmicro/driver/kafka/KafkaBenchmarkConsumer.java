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

import net.openhft.chronicle.threads.NamedThreadFactory;
import services.openmicro.driver.api.EventHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class KafkaBenchmarkConsumer implements Closeable {

    private final KafkaConsumer<String, byte[]> consumer;
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private volatile boolean running = true;
    private boolean autoCommit;

    public KafkaBenchmarkConsumer(KafkaConsumer<String, byte[]> consumer, Map<String, String> consumerConfig, EventHandler<KafkaEvent> callback) {
        this.consumer = consumer;
        this.executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("consumer", true));
        this.autoCommit = Boolean.parseBoolean(consumerConfig.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        this.consumerTask = this.executor.submit(() -> {
            while (running) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    for (ConsumerRecord<String, byte[]> record : records) {
                        callback.event(KafkaBenchmarkDriver.bytesToEvent(record.value()));

                        offsetMap.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    }

                    if (!autoCommit && !offsetMap.isEmpty()) {
                        // Async commit all messages polled so far
                        consumer.commitAsync(offsetMap, null);
                    }
                } catch (Exception e) {
                    LoggerFactory.getLogger(getClass()).error("exception occur while consuming message", e);
                }
            }
        });
    }

    @Override
    public void close() {
        running = true;
        executor.shutdown();
        try {
            consumerTask.get();
        } catch (InterruptedException | ExecutionException e) {
            LoggerFactory.getLogger(getClass()).error("Consumer error", e);
        }
        consumer.close();
    }
}
