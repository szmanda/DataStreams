package com.example.bigdata.tools;

import com.example.bigdata.model.LocData;
import com.example.bigdata.model.TaxiEvent;
import com.example.bigdata.model.TaxiLocEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnrichWithLocData extends RichMapFunction<TaxiEvent, TaxiLocEvent> {
    private final String locFilePath;
    private Map<Integer, LocData> locDataMap;

    public EnrichWithLocData(String locFilePath) {
        this.locFilePath = locFilePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        locDataMap = loadLocDataMap();
    }

    @Override
    public TaxiLocEvent map(TaxiEvent taxiEvent) throws Exception {
        int locationID = taxiEvent.getLocationID();
        LocData locData = locDataMap.get(locationID);

        String borough = (locData != null) ? locData.getBorough() : "Unknown";

        return new TaxiLocEvent(
                borough,
                taxiEvent.getLocationID(),
                taxiEvent.getTimestamp(),
                taxiEvent.getStartStop(),
                taxiEvent.getPassengerCount(),
                taxiEvent.getAmount()
        );
    }

    private Map<Integer, LocData> loadLocDataMap() throws IOException {
        Map<Integer, LocData> map = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(locFilePath))) {
            String line;
            boolean headerSkipped = false;

            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                String[] parts = line.split(",");
                if (parts.length == 4) {
                    int locationID = Integer.parseInt(parts[0]);
                    String borough = parts[1].replace("\"", "");
                    String zone = parts[2].replace("\"", "");
                    String serviceZone = parts[3].replace("\"", "");
                    LocData locData = new LocData(locationID, borough, zone, serviceZone);
                    map.put(locationID, locData);
                }
            }
        }
        return map;
    }
}
