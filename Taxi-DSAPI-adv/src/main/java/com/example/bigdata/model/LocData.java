package com.example.bigdata.model;

public class LocData {
    private int locationID;
    private String borough;
    private String zone;
    private String serviceZone;

    public LocData(int locationID, String borough, String zone, String serviceZone) {
        this.locationID = locationID;
        this.borough = borough;
        this.zone = zone;
        this.serviceZone = serviceZone;
    }

    // Konstruktor bezparametrowy
    public LocData() {
        // Domyślne inicjalizacje, można dostosować do potrzeb
        this.locationID = 0;
        this.borough = "";
        this.zone = "";
        this.serviceZone = "";
    }

    public int getLocationID() {
        return locationID;
    }

    public void setLocationID(int locationID) {
        this.locationID = locationID;
    }

    public String getBorough() {
        return borough;
    }

    public void setBorough(String borough) {
        this.borough = borough;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getServiceZone() {
        return serviceZone;
    }

    public void setServiceZone(String serviceZone) {
        this.serviceZone = serviceZone;
    }

    @Override
    public String toString() {
        return "LocData{" +
                "locationID=" + locationID +
                ", borough='" + borough + '\'' +
                ", zone='" + zone + '\'' +
                ", serviceZone='" + serviceZone + '\'' +
                '}';
    }
}
