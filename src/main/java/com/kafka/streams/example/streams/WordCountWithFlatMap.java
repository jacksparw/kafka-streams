package com.kafka.streams.example.streams;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

@Log4j
public class WordCountWithFlatMap {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCount =
                inputStream.mapValues(value -> value.toLowerCase())
                        .flatMap((key, value) -> Arrays.asList(value.split(" "))
                                .stream()
                                .map(word -> new KeyValue<>(word, word))
                                .collect(Collectors.toCollection(ArrayList::new)))
                        .groupBy((key, value) -> key)
                        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        wordCount.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaUtil.kafkaProperties());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
