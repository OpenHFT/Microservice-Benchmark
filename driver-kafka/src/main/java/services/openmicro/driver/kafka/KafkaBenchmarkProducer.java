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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import services.openmicro.driver.api.EventHandler;


public class KafkaBenchmarkProducer implements EventHandler<KafkaEvent> {

    private final KafkaProducer<String, byte[]>[] producers;
    private final String topic;
    private final int partitions;
    private int producer, partition;

    public KafkaBenchmarkProducer(KafkaProducer<String, byte[]>[] producers, String topic, int partitions) {
        this.producers = producers;
        this.topic = topic;
        this.partitions = partitions;
    }

    @Override
    public void event(KafkaEvent event) {
        try {
            byte[] payload = KafkaBenchmarkDriver.eventToBytes(event);
            final int producer, partition;
            synchronized (this) {
                producer = this.producer;
                partition = this.partition;
                if (++this.producer >= producers.length) {
                    this.producer = 0;
                    if (++this.partition >= partitions)
                        this.partition = 0;
                }
            }

            producers[producer].send(new ProducerRecord<>(topic, null, String.valueOf(partition), payload));

        } catch (Exception e) {
            throw new IORuntimeException(e);
        }
    }

    public void close() {
        Closeable.closeQuietly(producers);
    }
}
