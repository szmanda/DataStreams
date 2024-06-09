package com.example.bigdata.connectors;

//import com.example.bigdata.model.SensorDataAgg;
import com.example.bigdata.model.CombinedDelay;
import com.example.bigdata.model.Flight;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

public class Connectors {
    public static FileSource<String> getFileSource(ParameterTool properties) {
        return FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(properties.getRequired("fileInput.uri")))
                .monitorContinuously(Duration.ofMillis(
                        Long.parseLong(properties.getRequired("fileInput.interval"))))
                .build();
    }

    public static KafkaSource<String> getKafkaSource(ParameterTool properties) {
        return  KafkaSource.<String>builder()
                .setBootstrapServers(properties.getRequired("kafka.bootstrap"))
                .setTopics(properties.getRequired("kafka.topic"))
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSource<Flight> getKafkaSourceFlight(ParameterTool properties) {
        return  KafkaSource.<Flight>builder()
                .setBootstrapServers(properties.getRequired("kafka.bootstrap"))
                .setTopics(properties.getRequired("kafka.topic"))
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<Flight>() {
                    private final SimpleStringSchema stringDeser = new SimpleStringSchema();
                    @Override
                    public Flight deserialize(byte[] bytes) throws IOException {
                        String csvLine = stringDeser.deserialize(bytes);
                        return Flight.parseFromCsvLine(csvLine);
                    }
                })
                .build();
    }

    public static <T> SinkFunction<T> getPrintSink() {
        return new SinkFunction<T>() {
            @Override
            public void invoke(T value, Context context) {
                System.out.println(value.toString());
            }
        };
    }

    public static KafkaSink<String> getKafkaSink(ParameterTool properties) {
        Properties props = new Properties();
//        props.setProperty("transaction.timeout.ms", "7200000"); // e.g., 2 hour

        return KafkaSink.<String>builder()
                .setBootstrapServers(properties.getRequired("kafka.bootstrap"))
                .setKafkaProducerConfig(props)
//                 .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                 .setTransactionalIdPrefix("my-trx-id-prefix")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.getRequired("kafka.topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    public static SinkFunction<CombinedDelay> getMySQLSink(ParameterTool properties) {
        return JdbcSink.sink(
            "INSERT INTO delay_etl_image (state, arrival_count, departure_count, arrival_delay, departure_delay) VALUES (?, ?, ?, ?, ?)",
            new JdbcStatementBuilder<CombinedDelay>() {
                @Override
                public void accept(PreparedStatement ps, CombinedDelay delay) throws SQLException {
                    ps.setString(1, delay.getState());
                    ps.setInt(2, delay.getArrivalCount());
                    ps.setInt(3, delay.getDepartureCount());
                    ps.setInt(4, delay.getArrivalDelay());
                    ps.setInt(5, delay.getDepartureDelay());
                }
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(properties.getRequired("mysql.url"))
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername(properties.getRequired("mysql.username"))
                .withPassword(properties.getRequired("mysql.password"))
                .build()
        );
    }

//    public static SinkFunction<String> getKafkaSink(ParameterTool properties) {
//        Properties kafkaProperties = new Properties();
//        kafkaProperties.setProperty("bootstrap.servers", properties.getRequired("kafka.bootstrap"));
//
//        return new FlinkKafkaProducer<>(
//                properties.getRequired("kafka.topic"),
//                new SimpleStringSchema(),
//                kafkaProperties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//        );
//
//
////        return KafkaSink.<String>builder()
////                .setBootstrapServers(properties.getRequired("kafka.bootstrap"))
////                //.setKafkaTopic(properties.getRequired("kafka.topic"))
////                //.setValueSerializer(new SimpleStringSchema())
////                .build();
//    }

//    public static SinkFunction<SensorDataAgg> getMySQLSink(ParameterTool properties) {
//        JdbcStatementBuilder<SensorDataAgg> statementBuilder =
//                new JdbcStatementBuilder<SensorDataAgg>() {
//                    @Override
//                    public void accept(PreparedStatement ps, SensorDataAgg data) throws SQLException {
//                        ps.setString(1, data.getSensor());
//                        ps.setInt(2, data.getMaxVal());
//                        ps.setLong(3, data.getMaxValTimestamp());
//                        ps.setInt(4, data.getMinVal());
//                        ps.setLong(5, data.getMinValTimestamp());
//                        ps.setInt(6, data.getCountVal());
//                        ps.setInt(7, data.getSumVal());
//                    }
//                };
//        JdbcConnectionOptions connectionOptions = new
//                JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(properties.getRequired("mysql.url"))
//                .withDriverName("com.mysql.jdbc.Driver")
//                .withUsername(properties.getRequired("mysql.username"))
//                .withPassword(properties.getRequired("mysql.password"))
//                .build();
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
//                .withBatchSize(100)
//                .withBatchIntervalMs(200)
//                .withMaxRetries(5)
//                .build();
//        SinkFunction<SensorDataAgg> jdbcSink =
//                JdbcSink.sink("insert into sensor_data_sink" +
//                                "(sensor, max_val, max_val_timestamp, " +
//                                "min_val, min_val_timestamp, count_val, sum_val) \n" +
//                                "values (?, ?, ?, ?, ?, ?, ?)",
//                        statementBuilder,
//                        executionOptions,
//                        connectionOptions);
//        return jdbcSink;
//
//    }

}
