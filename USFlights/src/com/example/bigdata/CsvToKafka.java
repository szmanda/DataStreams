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

//        process(properties, env);
        exampleProcess(properties, env);

//        DataStream<String> eventStream = env.fromSource(Connectors.getKafkaSource(properties),
//                WatermarkStrategy.noWatermarks(), "KafkaInput");
//        eventStream.print();

        env.execute("CsvToKafka");
    }

    private static void process(ParameterTool properties, StreamExecutionEnvironment env) {
        DataStream<String> eventStream = env.fromSource(Connectors.getFileSource(properties),
                WatermarkStrategy.noWatermarks(), "CsvInput")
                .filter(a -> !a.startsWith("airline"))
                .map(a -> a.replace("\"\"", ""));

        eventStream.addSink(Connectors.<String>getPrintSink());
        eventStream.sinkTo(Connectors.getKafkaSink(properties));
        eventStream.print();
    }

    private static void exampleProcess(ParameterTool properties, StreamExecutionEnvironment env) {
        DataStream<String> exampleStream = env.fromElements(
                "OO,6604,N821AS,PHX,YUM,2015-01-01T11:00:00.000Z,4,59,2015-01-01 11:59:00,2015-01-01 10:56:00,17,,,,,,,,,,,,,2015-01-01 10:56:00,D",
                "EV,5488,N981EV,AEX,ATL,2015-01-01T11:06:00.000Z,4,95,2015-01-01 13:41:00,2015-01-01 10:56:00,9,,,,,,,,,,,,,2015-01-01 10:56:00,D",
                "AS,509,N644AS,SNA,SEA,2015-01-01T11:10:00.000Z,4,159,2015-01-01 13:49:00,2015-01-01 10:56:00,7,,,,,,,,,,,,,2015-01-01 10:56:00,D",
                "WN,2303,N8632A,DAL,LGA,2015-01-01T07:10:00.000Z,4,195,2015-01-01 11:25:00,2015-01-01 07:09:00,8,1381,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "UA,229,N428UA,IAD,SAN,2015-01-01T08:10:00.000Z,4,345,2015-01-01 10:55:00,2015-01-01 08:25:00,16,2253,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "UA,1142,N63820,MCO,DEN,2015-01-01T08:12:00.000Z,4,248,2015-01-01 10:20:00,2015-01-01 08:46:00,10,1546,22,2015-01-01 10:56:00,0,0,,2,0,34,0,0,,2015-01-01 10:56:00,A",
                "DL,2198,N6715C,MSP,DTW,2015-01-01T08:20:00.000Z,4,110,2015-01-01 11:10:00,2015-01-01 08:15:00,23,528,7,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "F9,1278,N918FR,MCO,PHL,2015-01-01T08:50:00.000Z,4,140,2015-01-01 11:10:00,2015-01-01 08:39:00,16,861,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "EV,4523,N14558,CVG,IAH,2015-01-01T08:54:00.000Z,4,171,2015-01-01 10:45:00,2015-01-01 08:52:00,20,871,10,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "B6,1024,N353JB,MCO,DCA,2015-01-01T08:55:00.000Z,4,124,2015-01-01 10:59:00,2015-01-01 08:50:00,17,759,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "DL,1660,N952DL,ATL,EWR,2015-01-01T08:55:00.000Z,4,130,2015-01-01 11:05:00,2015-01-01 09:00:00,11,746,4,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "UA,204,N820UA,IAH,SAT,2015-01-01T09:14:00.000Z,4,65,2015-01-01 10:19:00,2015-01-01 09:53:00,20,191,3,2015-01-01 10:56:00,0,0,,0,0,37,0,0,,2015-01-01 10:56:00,A",
                "B6,2379,N283JB,BOS,EWR,2015-01-01T09:35:00.000Z,4,80,2015-01-01 10:55:00,2015-01-01 09:38:00,21,200,5,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "WN,303,N735SA,LAS,SJC,2015-01-01T09:45:00.000Z,4,85,2015-01-01 11:10:00,2015-01-01 09:43:00,13,386,2,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "WN,3929,N707SA,BWI,BHM,2015-01-01T09:50:00.000Z,4,140,2015-01-01 11:10:00,2015-01-01 09:46:00,7,682,3,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "WN,2492,N8310C,DCA,MKE,2015-01-01T09:50:00.000Z,4,130,2015-01-01 11:00:00,2015-01-01 09:45:00,11,634,6,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "WN,778,N417WN,ALB,BWI,2015-01-01T09:55:00.000Z,4,85,2015-01-01 11:20:00,2015-01-01 09:50:00,9,289,2,2015-01-01 10:56:00,0,0,,,,,,,,2015-01-01 10:56:00,A",
                "AA,2210,N469AA,DFW,SAT,2015-01-01T10:05:00.000Z,4,70,2015-01-01 11:15:00,2015-01-01 10:05:00,,247,,,0,1,B,,,,,,2015-01-01 10:56:57,2015-01-01 10:56:57,C",
                "AA,120,N3GYAA,LAS,JFK,2015-01-01T10:15:00.000Z,4,300,2015-01-01 18:15:00,2015-01-01 10:57:00,21,,,,,,,,,,,,,2015-01-01 10:57:00,D",
                "WN,397,N8328A,BWI,LAX,2015-01-01T10:35:00.000Z,4,365,2015-01-01 13:40:00,2015-01-01 10:57:00,22,,,,,,,,,,,,,2015-01-01 10:57:00,D",
                "OO,3468,N224AG,SEA,OMA,2015-01-01T10:40:00.000Z,4,180,2015-01-01 15:40:00,2015-01-01 10:57:00,14,,,,,,,,,,,,,2015-01-01 10:57:00,D"
        );
        exampleStream.addSink(Connectors.<String>getPrintSink());
        exampleStream.sinkTo(Connectors.getKafkaSink(properties));
        exampleStream.print();
    }
}
