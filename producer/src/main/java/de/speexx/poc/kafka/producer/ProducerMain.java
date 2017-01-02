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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMain {
    
    public static final String TOPIC = "firsttopic";
    public static final String KAFKA_IP = "192.168.5.200";
    public static final String KAFKA_PORT = "9092";
    
    public static void main(final String... args) throws Exception {
        
        final Properties props = producerConfiguration();

        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.format("Producer %s created.%n", producer);
            final ProducerRecord record = new ProducerRecord<>(TOPIC, "time-" + System.currentTimeMillis(),
                                                                      "Hello " + System.getProperty("user.name"));
            producer.send(record);
            System.out.format("Record %s sent.%n", record);
            producer.flush();
        }
    }

    static Properties producerConfiguration() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_IP + ":" + KAFKA_PORT);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
