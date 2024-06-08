package com.example.bigdata.model;

import java.io.Serializable;

public class Airport implements Serializable {
    private Integer airportId;
    private String name;
    private String city;
    private String country;
    private String IATA;
    private String ICAO;
    private Double latitude;
    private Double longitude;
    private Integer altitude;
    private String timezone;
    private String DST;
    private String timezoneName;
    private String type;
    private String state;

    public Airport() {
        this(-1, "", "", "", "", "", 0.0, 0.0, -1, "", "", "", "", "");
    }

    public Airport(Integer airportId, String name, String city, String country, String IATA, String ICAO, Double latitude, Double longitude, Integer altitude, String timezone, String DST, String timezoneName, String type, String state) {
        this.airportId = airportId;
        this.name = name;
        this.city = city;
        this.country = country;
        this.IATA = IATA;
        this.ICAO = ICAO;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.timezone = timezone;
        this.DST = DST;
        this.timezoneName = timezoneName;
        this.type = type;
        this.state = state;
    }

    @Override
    public String toString() {
        return "Airport{" +
                "airportId=" + airportId +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                ", country='" + country + '\'' +
                ", IATA='" + IATA + '\'' +
                ", ICAO='" + ICAO + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", altitude=" + altitude +
                ", timezone='" + timezone + '\'' +
                ", DST='" + DST + '\'' +
                ", timezoneName='" + timezoneName + '\'' +
                ", type='" + type + '\'' +
                ", state='" + state + '\'' +
                '}';
    }

    public Integer get0() {
        return 0;
    }

    public Integer getAirportId() {
        return airportId;
    }

    public void setAirportId(Integer airportId) {
        this.airportId = airportId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getIATA() {
        return IATA;
    }

    public void setIATA(String IATA) {
        this.IATA = IATA;
    }

    public String getICAO() {
        return ICAO;
    }

    public void setICAO(String ICAO) {
        this.ICAO = ICAO;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Integer getAltitude() {
        return altitude;
    }

    public void setAltitude(Integer altitude) {
        this.altitude = altitude;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getDST() {
        return DST;
    }

    public void setDST(String DST) {
        this.DST = DST;
    }

    public String getTimezoneName() {
        return timezoneName;
    }

    public void setTimezoneName(String timezoneName) {
        this.timezoneName = timezoneName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
