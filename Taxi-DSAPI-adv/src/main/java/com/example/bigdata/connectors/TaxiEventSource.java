package com.example.bigdata.connectors;

import com.example.bigdata.model.TaxiEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;

public class TaxiEventSource implements SourceFunction<TaxiEvent> {
    private final String directoryPath;
    private final long elementDelayMillis;
    private final int maxElements;
    private transient volatile boolean running = true;

    public TaxiEventSource(String directoryPath, long elementDelayMillis, int maxElements) {
        this.directoryPath = directoryPath;
        this.elementDelayMillis = elementDelayMillis;
        this.maxElements = maxElements;
    }

    public TaxiEventSource(ParameterTool properties) {
        this.directoryPath = properties.get("taxiEvents.directoryPath");
        this.elementDelayMillis = Long.parseLong(properties.get("taxiEvents.elementDelayMillis"));
        this.maxElements = Integer.parseInt(properties.get("taxiEvents.maxElements"));
    }

    @Override
    public void run(SourceContext<TaxiEvent> sourceContext) throws Exception {
        running = true;
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        if (files != null) {
            java.util.Arrays.sort(files);

            int elementCount = 0;

            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".csv")) {
                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line;
                        boolean headerSkipped = false;

                        while ((line = reader.readLine()) != null && running) {
                            if (!headerSkipped) {
                                headerSkipped = true;
                                continue;
                            }

                            try {
                                TaxiEvent taxiEvent = TaxiEvent.fromString(line);
                                sourceContext.collectWithTimestamp(taxiEvent,taxiEvent.getTimestamp().getTime());
                                elementCount++;
                                if (elementCount >= maxElements) {
                                    cancel();
                                    break;
                                }
                                Thread.sleep(elementDelayMillis);
                            } catch (ParseException e) {
                                // Print malformed line to stderr
                                System.err.println("Malformed line: " + line);
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

