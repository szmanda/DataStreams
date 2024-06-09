package com.example.bigdata;

import com.example.bigdata.connectors.Connectors;
import com.example.bigdata.model.Airport;
import com.example.bigdata.model.CombinedDelay;
import com.example.bigdata.model.Flight;
import com.example.bigdata.transformations.DelayAggregate;
import com.example.bigdata.transformations.FlightAggregate;
import com.example.bigdata.transformations.FlightAirportToCombinedDelay;
import com.example.bigdata.watermarks.AirportWatermarkStrategy;
import com.example.bigdata.watermarks.DelayWatermarkStrategy;
import com.example.bigdata.watermarks.DelayWindowAssigner;
import com.example.bigdata.watermarks.FlightWatermarkStrategy;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
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
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
//        KStream<String, String> textLines = builder.stream(properties.getRequired("kafka.topic"));
//        flightsString.sinkTo(Connectors.getKafkaSink(properties));
//        flightsString.print();

        Map<String, Airport> airportsMap = loadAirports(properties, env);
//        Map<String, Airport> airportsMap = new HashMap<>();
//        DataStream<String> flightsString = env.fromSource(Connectors.getKafkaSource(properties),
//                WatermarkStrategy.noWatermarks(), "KafkaInput");

//        DataStream<String> flightsString = env.fromElements(
//                "OO,6604,N821AS,PHX,YUM,2015-01-01T11:00:00.000Z,4,59,2015-01-01 11:59:00,2015-01-01 10:56:00,17,,,,,,,,,,,,,2015-01-01 10:56:00,D",
//                "EV,5488,N981EV,AEX,ATL,2015-01-01T11:06:00.000Z,4,95,2015-01-01 13:41:00,2015-01-01 10:56:00,9,,,,,,,,,,,,,2015-01-01 11:56:00,D",
//                "AS,509,N644AS,SNA,SEA,2015-01-01T11:10:00.000Z,4,159,2015-01-01 13:49:00,2015-01-01 10:56:00,7,,,,,,,,,,,,,2015-01-01 12:56:00,D",
//                "WN,2303,N8632A,DAL,LGA,2015-01-01T07:10:00.000Z,4,195,2015-01-01 11:25:00,2015-01-01 07:09:00,8,1381,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-02 10:56:00,A",
//                "UA,229,N428UA,IAD,SAN,2015-01-01T08:10:00.000Z,4,345,2015-01-01 10:55:00,2015-01-01 08:25:00,16,2253,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-02 10:56:00,A",
//                "UA,1142,N63820,MCO,DEN,2015-01-01T08:12:00.000Z,4,248,2015-01-01 10:20:00,2015-01-01 08:46:00,10,1546,22,2015-01-01 10:56:00,0,0,,2,0,34,0,0,,2015-01-02 10:56:00,A",
//                "DL,2198,N6715C,MSP,DTW,2015-01-01T08:20:00.000Z,4,110,2015-01-01 11:10:00,2015-01-01 08:15:00,23,528,7,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-03 10:56:00,A",
//                "F9,1278,N918FR,MCO,PHL,2015-01-01T08:50:00.000Z,4,140,2015-01-01 11:10:00,2015-01-01 08:39:00,16,861,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-03 10:56:00,A",
//                "EV,4523,N14558,CVG,IAH,2015-01-01T08:54:00.000Z,4,171,2015-01-01 10:45:00,2015-01-01 08:52:00,20,871,10,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-03 10:56:00,A",
//                "B6,1024,N353JB,MCO,DCA,2015-01-01T08:55:00.000Z,4,124,2015-01-01 10:59:00,2015-01-01 08:50:00,17,759,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-03 12:56:00,A",
//                "DL,1660,N952DL,ATL,EWR,2015-01-01T08:55:00.000Z,4,130,2015-01-01 11:05:00,2015-01-01 09:00:00,11,746,4,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-03 12:56:00,A",
//                "UA,204,N820UA,IAH,SAT,2015-01-01T09:14:00.000Z,4,65,2015-01-01 10:19:00,2015-01-01 09:53:00,20,191,3,2015-01-01 10:56:00,0,0,,0,0,37,0,0,,2015-01-03 12:56:00,A",
//                "B6,2379,N283JB,BOS,EWR,2015-01-01T09:35:00.000Z,4,80,2015-01-01 10:55:00,2015-01-01 09:38:00,21,200,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-04 10:56:00,A",
//                "WN,303,N735SA,LAS,SJC,2015-01-01T09:45:00.000Z,4,85,2015-01-01 11:10:00,2015-01-01 09:43:00,13,386,2,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-04 10:56:00,A",
//                "WN,3929,N707SA,BWI,BHM,2015-01-01T09:50:00.000Z,4,140,2015-01-01 11:10:00,2015-01-01 09:46:00,7,682,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-04 10:56:00,A",
//                "WN,2492,N8310C,DCA,MKE,2015-01-01T09:50:00.000Z,4,130,2015-01-01 11:00:00,2015-01-01 09:45:00,11,634,6,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-05 10:56:00,A",
//                "WN,778,N417WN,ALB,BWI,2015-01-01T09:55:00.000Z,4,85,2015-01-01 11:20:00,2015-01-01 09:50:00,9,289,2,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-05 10:56:00,A",
//                "AA,2210,N469AA,DFW,SAT,2015-01-01T10:05:00.000Z,4,70,2015-01-01 11:15:00,2015-01-01 10:05:00,,247,,,0,1,B,,,,,,2015-01-01 10:56:57,2015-01-05 10:56:57,C",
//                "AA,120,N3GYAA,LAS,JFK,2015-01-01T10:15:00.000Z,4,300,2015-01-01 18:15:00,2015-01-01 10:57:00,21,,,,,,,,,,,,,2015-01-05 10:57:00,D",
//                "WN,397,N8328A,BWI,LAX,2015-01-01T10:35:00.000Z,4,365,2015-01-01 13:40:00,2015-01-01 10:57:00,22,,,,,,,,,,,,,2015-01-05 10:57:00,D",
//                "OO,3468,N224AG,SEA,OMA,2015-01-01T10:40:00.000Z,4,180,2015-01-01 15:40:00,2015-01-01 10:57:00,14,,,,,,,,,,,,,2015-01-05 10:57:00,D"
//        );

//        DataStream<Flight> flights = flightsString.map(Flight::parseFromCsvLine)
//                .assignTimestampsAndWatermarks(new FlightWatermarkStrategy());
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Flight>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner((SerializableTimestampAssigner<Flight>)
//                                        (element, recordTimestamp) -> {
//                                            try {
//                                                return (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(element.getOrderColumn()).getTime();
//                                            } catch (ParseException e) {
//                                                System.out.println("setting current time instead:"+recordTimestamp);
//                                                throw new RuntimeException("cannot parse date for flights");
////                                                return recordTimestamp;
//                                            }
//                                        })
//                );


        DataStream<Flight> flights = env.fromSource(
                Connectors.getKafkaSourceFlight(properties),
                new FlightWatermarkStrategy(),
                "FlightsKafkaInput").setParallelism(1);
        // Note: without .setParallelism(1), watermarks are being messed up after keyBy()

//        flights.print();

        FlightAggregate flightAggregate = new FlightAggregate();
        flightAggregate.setAirportsMap(airportsMap);

        DataStream<CombinedDelay> aggregated = flights
                .keyBy(Flight::getCurrentAirport)
//                .window(new DelayWindowAssigner("A"))
                .window(new DelayWindowAssigner("C"))
                .aggregate(flightAggregate)
                ;

//        aggregated.process(new ProcessFunction<CombinedDelay, Object>() {
//            @Override
//            public void processElement(CombinedDelay combinedDelay, ProcessFunction<CombinedDelay, Object>.Context context, Collector<Object> collector) throws Exception {
//                System.out.println("aggregated_WM: " +new Date(context.timerService().currentWatermark()));
//            }
//        });

//        aggregated.print();



/*
        DataStream<CombinedDelay> combinedDelayStream = flights.map(flight -> {
            Airport airport = airportsMap.get(flight.getCurrentAirport());

            CombinedDelay combined = new CombinedDelay();
//            // // combined.setAirport(flight.getCurrentAirport());
            combined.setDelay(flight.getTotalDelayInteger());
            combined.setInfoType(flight.getInfoType());
            combined.setState(airport.getState());
            combined.setDate(flight.getOrderColumnDate());
            combined.setTimeZone(Integer.parseInt(airport.getTimezone()));

            return combined;
        })
//        .assignTimestampsAndWatermarks(
//                new DelayWatermarkStrategy()
////                WatermarkStrategy.<CombinedDelay>forBoundedOutOfOrderness(Duration.ofMinutes(1))
////                        .withTimestampAssigner((SerializableTimestampAssigner<CombinedDelay>)
////                                (element, recordTimestamp) -> {
//                          // System.out.println(element.getUtcDate().getTime());
//                          return (element.getUtcDate().getTime()); })
//        )
        ;

        DataStream<CombinedDelay> timestamped = combinedDelayStream
                .assignTimestampsAndWatermarks(new DelayWatermarkStrategy());

        KeyedStream<CombinedDelay, String> keyed = timestamped
                .keyBy(CombinedDelay::getState);

//        WindowedStream<CombinedDelay, String, TimeWindow> windowed = keyed
//            .window(new DelayWindowAssigner("C"));
//
//        DataStream<CombinedDelay> aggregated = windowed
//                .aggregate(new DelayAggregate());

        timestamped.process(new ProcessFunction<CombinedDelay, Object>() {
            @Override
            public void processElement(CombinedDelay combinedDelay, ProcessFunction<CombinedDelay, Object>.Context context, Collector<Object> collector) throws Exception {
                System.out.println("timestamped_WM: " +new Date(context.timerService().currentWatermark()));
            }
        });

        keyed.process(new ProcessFunction<CombinedDelay, Object>() {
            @Override
            public void processElement(CombinedDelay combinedDelay, ProcessFunction<CombinedDelay, Object>.Context context, Collector<Object> collector) throws Exception {
                System.out.println("keyed_WM: " +new Date(context.timerService().currentWatermark()));
            }
        });

//        DataStream<CombinedDelay> combinedDelayDataStreamTS = combinedDelayStream
//                .assignTimestampsAndWatermarks(new DelayWatermarkStrategy());

//        combinedDelayDataStreamTS
//                .keyBy(CombinedDelay::getState)
//                .process(new ProcessFunction<CombinedDelay, String>() {
//            @Override
//            public void processElement(CombinedDelay value, Context ctx, Collector<String> out) throws Exception {
//                long currentWatermark = ctx.timerService().currentWatermark();
//                System.out.println("CurWM: "+ new Date(currentWatermark) + ", : " + value);
//                out.collect("output");
//            }
//        });

//        DataStream<CombinedDelay> aggregatedDelayStream = combinedDelayDataStreamTS
//            .keyBy(CombinedDelay::getState) // !!!!!!!!! KeyBy messes up the watermarks, for some reason !!!
//            .window(new DelayWindowAssigner("A"))
//            .window(new DelayWindowAssigner("C"))
//            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//            .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(1)))
//            .aggregate(new CombinedDelay())
//            .map(a -> { System.out.println("AGG -> " + a + " with timestamp -> " + a.getUtcDate().getTime()); return a; })
//            .assignTimestampsAndWatermarks(
//                    new DelayWatermarkStrategy()
////                WatermarkStrategy.<CombinedDelay>forBoundedOutOfOrderness(Duration.ofMinutes(1))
////                    .withTimestampAssigner((SerializableTimestampAssigner<CombinedDelay>)
////                        (element, recordTimestamp) -> {
////                            System.out.println(element.getUtcDate().getTime());
////                            return (element.getUtcDate().getTime()); })
//            )
//        ;

//        try {
//            CloseableIterator<CombinedDelay> combinedDelayCloseableIterator = aggregatedDelayStream.executeAndCollect();
//
//            System.out.println("has any results: " + combinedDelayCloseableIterator.hasNext());
//
//            combinedDelayCloseableIterator.forEachRemaining(System.out::println);
//
//
//        } catch (Exception e) {
//            System.out.println("AGG------------------");
//            throw new RuntimeException("AGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG");
//        }
//
//        aggregatedDelayStream.print();



 */


        env.execute("USFlightsApp");

        // // // // // // //

//        Properties config = new Properties();
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.bootstrap"));
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "us-filghts-application");
//        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

//        final Topology topology = builder.build();
//        System.out.println(topology.describe());


//        env.execute("USFlightsApp");
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

    private static Map<String, Airport> loadAirports(ParameterTool properties, StreamExecutionEnvironment env) {
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
                ));

//        airports.print();

        Map<String, Airport> airportMap = new HashMap<>();
        Iterator<Airport> iterator = null;
        try {
            iterator = airports.executeAndCollect();
            while (iterator.hasNext()) {
                Airport a = iterator.next();
                airportMap.put(a.getIATA(), a);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

//        for (Map.Entry<String, Airport> entry : airportMap.entrySet()) {
//            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
//        }

        return airportMap;
    }
}

