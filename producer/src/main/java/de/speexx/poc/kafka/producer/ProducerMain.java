/* Simple example to work with a Apache Kafka producer consumer environment.
 *
 * Copyright (C) 2017 Sascha Kohlmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package de.speexx.poc.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {
    
    private static final Logger LOG = LoggerFactory.getLogger("PRODUCER");

    public static final String TOPIC = "firsttopic";
    public static final String KAFKA_IP = "192.168.5.200";
    public static final String KAFKA_PORT = "9092";
    
    public static void main(final String... args) throws Exception {
        
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP + ":" + KAFKA_PORT);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.format("Producer %s created.%n", producer);
            final ProducerRecord record = new ProducerRecord<>(TOPIC, "time-" + System.currentTimeMillis(), "Hello world");
            producer.send(record);
            System.out.format("Record %s sent.%n", record);
        }
    }
}
