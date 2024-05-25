package com.example.bigdata;

import com.example.bigdata.connectors.Connectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CsvToKafka {

    public static void main(String[] args) throws Exception {

        ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile("flink.properties");
        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);
        ParameterTool properties = propertiesFromFile.mergeWith(propertiesFromArgs);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        process(properties, env);

//        DataStream<String> eventStream = env.fromSource(Connectors.getKafkaSource(properties),
//                WatermarkStrategy.noWatermarks(), "KafkaInput");
//        eventStream.print();

        env.execute("CsvToKafka");
    }

    private static void process(ParameterTool properties, StreamExecutionEnvironment env) {
        DataStream<String> eventStream = env.fromSource(Connectors.getFileSource(properties),
                WatermarkStrategy.noWatermarks(), "CsvInput");

        eventStream.addSink(Connectors.getPrintSink());
        eventStream.sinkTo(Connectors.getKafkaSink(properties));
        eventStream.print();
    }

    private static void exampleProcess(ParameterTool properties, StreamExecutionEnvironment env) {
        DataStream<String> exampleStream = env.fromElements(
                "73,altitude,1591531560000",
                "63,cadence,1591531560000",
                "83,heartrate,1591531560000",
                "21,rotations,1591531560000",
                "33,speed,1591531560000",
                "26,temperature,1591531560000",
                "73,altitude,1591531561000",
                "63,cadence,1591531561000",
                "83,heartrate,1591531561000",
                "21,rotations,1591531561000",
                "33,speed,1591531561000",
                "26,temperature,1591531561000");

        exampleStream.print();
    }
}
