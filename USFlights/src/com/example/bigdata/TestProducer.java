package com.example.bigdata;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class TestProducer {
    public static void main(String[] args) throws IOException {
        ParameterTool properties = ParameterTool.fromPropertiesFile("flink.properties");


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties.getProperties());
        final File folder = new File(properties.get("fileInput.dir"));
        File[] listOfFiles = folder.listFiles();
        String[] listOfPaths = Arrays.stream(listOfFiles).
                map(file -> file.getAbsolutePath()).toArray(String[]::new);
        Arrays.sort(listOfPaths);
        for (final String fileName : listOfPaths) {
            try (Stream<String> stream = Files.lines(Paths.get(fileName)).
                    skip(0)) {
                stream.forEach(line ->
                        {
                            if (line.startsWith("airline,")) return;
//                            try { TimeUnit.MILLISECONDS.sleep(300); } catch (InterruptedException e) {}
                            System.out.println(line);
                            producer.send(
                                    new ProducerRecord<>(properties.get("kafka.topic"), String.valueOf(line.hashCode()), line)
                            );

                        }
                );
                TimeUnit.SECONDS.sleep(15);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}