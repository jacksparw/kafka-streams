package com.kafka.streams.example.streams.favColor;

import com.kafka.streams.example.streams.KafkaUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavColorWithAggregate {

    public static final List<String> ALLOWED_COLORS = Arrays.asList("red", "blue", "green");

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stringRecords = builder.stream("favourite-colour-input");

        stringRecords.filter((key, value) -> value.contains(","))
                .map((key, value) -> new KeyValue<>(value.split(",")[0], value.split(",")[1]))
                .filter((user, color) -> ALLOWED_COLORS.contains(color)).to("intermediate-favourite-color");

        builder.<String, String>table("intermediate-favourite-color")
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .aggregate(
                        () -> 0L,
                        (key, newValue, aggregate) -> aggregate + 1L,
                        (key, oldValue, aggregate) -> aggregate - 1L,
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("favouriteCountByColors")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                )
                .toStream()
                .to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        Properties config = KafkaUtil.kafkaProperties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
