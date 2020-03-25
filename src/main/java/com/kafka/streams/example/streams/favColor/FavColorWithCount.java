package com.kafka.streams.example.streams.favColor;

import com.kafka.streams.example.streams.KafkaUtil;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavColorWithCount {

    public static final List<String> ALLOWED_COLORS = Arrays.asList("red", "blue", "green");

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stringRecords = builder.stream("user-color-input");

        stringRecords.filter((key, value) -> value.contains(","))
                .map((key, value) -> new KeyValue<>(value.split(",")[0], value.split(",")[1]))
                .filter((user, color) -> ALLOWED_COLORS.contains(color)).to("intermediate_fav_color");


        KTable<String, String> fav_color_KTable = builder.table("intermediate_fav_color");

        fav_color_KTable
                .groupBy((key, value) -> new KeyValue<>(value,value))
                .count(Materialized.<String, Long, KeyValueStore< Bytes, byte[]>>as("CountByColors"));

        Properties config = KafkaUtil.kafkaProperties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-user-application");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
