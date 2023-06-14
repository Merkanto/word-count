package com.merkanto.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-colour-java");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Step 1: We create the topic of users keys to colours
        KStream<String,String> textLines = streamsBuilder.stream("favourite-colour-input");

        KStream<String,String> userAndColours = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, val) -> val.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, val) -> val.split(",")[0].toLowerCase())
                // 3 - we get the colour from the value (lowercase for safety)
                .mapValues(val -> val.split(",")[1].toLowerCase())
                // 4 - we filter undesired colours (could be a data sanitization step
                .filter((user, colour) -> (Arrays.asList("blue","red","green").contains(colour)));

        userAndColours.to("user-keys-and-colours");

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColoursTable = streamsBuilder.table("user-keys-and-colours");

        // step 3 - we count the occurrences of colours
        KTable<String, Long> favouriteColours = usersAndColoursTable
                // 5 - we group by colour within the KTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Named.as("CountsByColours"));

//        stringLongKTable.toStream().print(Printed.toSysOut());

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favouriteColours.toStream().to("favourite-colour-output", Produced.with(Serdes.String(),Serdes.Long()));


        streamsBuilder.stream("favourite-colour-output").print(Printed.toSysOut());
/*
example output of above line
[KSTREAM-SOURCE-0000000016]: red,
[KSTREAM-SOURCE-0000000016]: red,

*/

        Topology topology = streamsBuilder.build();

        KafkaStreams kafkaStreams = new KafkaStreams(topology,config);
        //only on dev - not in prod
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        // print the topology
        System.out.println(kafkaStreams);

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
