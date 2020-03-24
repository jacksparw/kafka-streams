package com.kafka.streams.example.streams;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;

@Log4j
public class WordCount {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCount =
                inputStream.mapValues(value -> value.toLowerCase())
                        .flatMapValues(value -> Arrays.asList(value.split(" ")))
                        .selectKey((key, value) -> value)
                        .groupBy((key, value) -> key)
                        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        wordCount.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaUtil.kafkaProperties());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
