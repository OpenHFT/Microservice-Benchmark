/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package services.openmicro.driver.kafka;

import net.openhft.chronicle.core.io.IORuntimeException;
import services.openmicro.driver.api.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaBenchmarkProducer implements EventHandler<KafkaEvent> {

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaBenchmarkProducer(KafkaProducer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void event(KafkaEvent event) {
        try {
            byte[] payload = KafkaBenchmarkDriver.eventToBytes(event);
            producer.send(new ProducerRecord<>(topic, payload));
        } catch (Exception e) {
            throw new IORuntimeException(e);
        }
    }

    public void close() {
        producer.close();
    }
}
