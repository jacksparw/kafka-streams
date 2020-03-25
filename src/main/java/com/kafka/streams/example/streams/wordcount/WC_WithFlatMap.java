package com.kafka.streams.example.streams.wordcount;

import com.kafka.streams.example.streams.KafkaUtil;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

@Log4j
public class WC_WithFlatMap {

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCount =
                inputStream.mapValues(value -> value.toLowerCase())
                        .flatMap((key, value) -> Arrays.asList(value.split(" "))
                                .stream()
                                .map(word -> KeyValue.pair(word, word))
                                .collect(Collectors.toList()))
                        .groupByKey()
                        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

        wordCount.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = KafkaUtil.kafkaProperties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
