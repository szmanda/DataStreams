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

        Map<String, Airport> airportsMap = loadAirports(properties, env);

        DataStream<Flight> inputStream = env.fromSource(
                Connectors.getKafkaSourceFlight(properties),
                new FlightWatermarkStrategy(),
                "FlightsKafkaInput").setParallelism(1);
        // Note: without .setParallelism(1), watermarks are being messed up after keyBy()

//        flights.print();

        DataStream<Flight> flights = inputStream
                .map(new MapFunction<Flight, Flight>() {
                    @Override
                    public Flight map(Flight flight) throws Exception {
                        Airport airport = airportsMap.get(flight.getCurrentAirport());
                        if (airport == null) {
//                            System.out.println("WARNING: Airport not found!");
                            airport = new Airport();
                        }
                        flight.setState(airport.getState());
                        flight.setTimeZone(airport.getTimezone());
                        return flight;
                    }
                });

        FlightAggregate flightAggregate = new FlightAggregate();
        flightAggregate.setAirportsMap(airportsMap);

        DataStream<CombinedDelay> aggregated = flights
                .keyBy(Flight::getState)
//                .window(new DelayWindowAssigner("A"))
                .window(new DelayWindowAssigner(properties.get("update.mode")))
                .aggregate(flightAggregate)
                ;

        if (!properties.get("mysql.disableSink", "false").equals("true")) {
            aggregated.addSink(Connectors.getMySQLSink(properties));
        }
        aggregated.print();

        env.execute("USFlightsApp");
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

