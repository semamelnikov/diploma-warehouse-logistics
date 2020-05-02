/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.kpfu.itis.wordcount;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import ru.kpfu.itis.wordcount.avro.WordCountRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurence histogram, write the continuous updated histogram
 * into a topic "streams-wordcount-output" where each record is an updated count of a single word.
 */

public class WordCount {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        SpecificAvroSerde<WordCountRecord> wordCountAvroSerde = new SpecificAvroSerde<>();
        wordCountAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081"), false);

        final StreamsBuilder builder = new StreamsBuilder();

        System.out.println("Hello World!");

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.peek((key, value) -> System.out.println(value))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("counts-store"))
                .toStream()
                .map((key, value) -> KeyValue.pair(key, new WordCountRecord(key, value)))
                .peek((key, value) -> System.out.println("Key = " + key + " , WordCountRecord: {word:" + value.getWord() + ", count:" + value.getCount() + "}"))
                .to("streams-wordcount-output", Produced.with(Serdes.String(), wordCountAvroSerde));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
