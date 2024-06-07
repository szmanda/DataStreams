package com.example.bigdata;

import com.example.bigdata.connectors.Connectors;
import com.example.bigdata.model.Airport;
import com.example.bigdata.model.CombinedDelay;
import com.example.bigdata.model.Flight;
import com.example.bigdata.transformations.FlightAirportToCombinedDelay;
import com.example.bigdata.watermarks.AirportWatermarkStrategy;
import com.example.bigdata.watermarks.DelayWatermarkStrategy;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import javax.xml.crypto.Data;
import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.mysql.cj.protocol.a.MysqlTextValueDecoder.getTimestamp;

public class USFlightsApp {

    public static void main(String[] args) throws Exception {

        ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile("flink.properties");
        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);
        ParameterTool properties = propertiesFromFile.mergeWith(propertiesFromArgs);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        process(properties, env);


        System.exit(0);
    }

    private static void process(ParameterTool properties, StreamExecutionEnvironment env) throws Exception {
//        DataStream<String> eventStream = env.fromSource(Connectors.getFileSource(properties),
//                WatermarkStrategy.noWatermarks(), "CsvInput");
//
////        eventStream.addSink(Connectors.getPrintSink());
////        eventStream.sinkTo(Connectors.getKafkaSink(properties));
////        eventStream.print();
//
//        final StreamsBuilder builder = new StreamsBuilder();

//        KStream<String, String> textLines = builder.stream(properties.getRequired("kafka.topic"));
//
////        textLines.foreach((key, value) -> System.out.println(value));
//
//
//        final Topology topology = builder.build();
//        System.out.println(topology.describe());
//
//        // // // // // // // // // // //
//
//
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
//                properties.get("kafka.topic"),
//                new SimpleStringSchema(),
//                properties.getProperties()
//        );

        String path = properties.get("airports.input");
        DataStream<String> airportsCSV = env.readTextFile(path);
        DataStream<Airport> airports = airportsCSV
            .filter(a -> !a.startsWith("Airport ID"))
            .map(a -> a.split(","))
            .filter(a -> a.length == 14)
            .map(a -> new Airport(
                Integer.parseInt(a[0]),   // airportId
                a[1],                     // name
                a[2],                     // city
                a[3],                     // country
                a[4],                     // IATA
                a[5],                     // ICAO
                Double.parseDouble(a[6]), // latitude
                Double.parseDouble(a[7]), // longitude
                Integer.parseInt(a[8]),   // altitude
                a[9],                     // timezone
                a[10],                    // DST
                a[11],                    // timezoneName
                a[12],                    // type
                a[13]                     // state
            ))
            .assignTimestampsAndWatermarks(
//                    WatermarkStrategy.noWatermarks()
                    WatermarkStrategy.<Airport>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                            .withTimestampAssigner((SerializableTimestampAssigner<Airport>)
                                    (element, recordTimestamp) -> System.currentTimeMillis())
            );

//        airports.print();



//        KStream<String, String> textLines = builder.stream(properties.getRequired("kafka.topic"));
//
        DataStream<String> flightsString = env.fromSource(Connectors.getKafkaSource(properties),
                WatermarkStrategy.noWatermarks(), "KafkaInput");
//        flightsString.sinkTo(Connectors.getKafkaSink(properties));
//        flightsString.print();

        DataStream<Flight> flights = flightsString.map(Flight::parseFromCsvLine)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Flight>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<Flight>)
                                        (element, recordTimestamp) -> {
                                            try {
                                                return (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(element.getOrderColumn()).getTime();
                                            } catch (ParseException e) {
                                                return recordTimestamp;
                                            }
                                        })
                );
//        flights.print();

        DataStream<CombinedDelay> combinedDelay = flights
                .join(airports)
                .where(Flight::getCurrentAirport)
                .equalTo(Airport::getIATA)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new JoinFunction<Flight, Airport, CombinedDelay>() {
                    @Override
                    public CombinedDelay join(Flight flight, Airport airport) {
                        CombinedDelay delay = new CombinedDelay();
                        delay.setFlightNumber(flight.getFlightNumber());
                        delay.setAirport(flight.getCurrentAirport());
                        delay.setAirportIATA(airport.getIATA());

                        return delay;
                    }
                });
         
//        combinedDelay.addSink(Connectors.getPrintSink());
//        combinedDelay.print();


        try {
            env.execute("print connectors");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        // // // // // // //

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.bootstrap"));
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "us-filghts-application");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

//        final Topology topology = builder.build();
//        System.out.println(topology.describe());


        env.execute("USFlightsApp");
//        KafkaStreams streams = new KafkaStreams(topology, config);
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//        System.exit(0);
        
    }
}

