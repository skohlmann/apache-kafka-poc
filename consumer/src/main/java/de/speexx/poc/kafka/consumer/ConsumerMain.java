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
package de.speexx.poc.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMain {

    private static final Logger LOG = LoggerFactory.getLogger("CONSUMER");
    
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
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            System.out.format("Consumer %s created.%n", consumer);
            consumer.subscribe(Arrays.asList(TOPIC));
            System.out.format("Start polling.%n");
            final ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.format("ConsumerRecords with %d entries.%n", records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.format("From topic: %s: Key: %s with value: %s%n", record.topic(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }
}
