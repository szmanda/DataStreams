package com.example.bigdata.model;

public class CombinedDelay {

    private String flightNumber;
    private String infoType;
    private String airport;
    private String airportIATA;
    private String state;

    @Override
    public String toString() {
        return "CombinedDelay{" +
                "flightNumber='" + flightNumber + '\'' +
                ", infoType='" + infoType + '\'' +
                ", airport='" + airport + '\'' +
                ", airportIATA='" + airportIATA + '\'' +
                ", state='" + state + '\'' +
                '}';
    }

    public String getFlightNumber() {
        return flightNumber;
    }

    public void setFlightNumber(String flightNumber) {
        this.flightNumber = flightNumber;
    }

    public String getInfoType() {
        return infoType;
    }

    public void setInfoType(String infoType) {
        this.infoType = infoType;
    }

    public String getAirport() {
        return airport;
    }

    public void setAirport(String airport) {
        this.airport = airport;
    }

    public String getAirportIATA() {
        return airportIATA;
    }

    public void setAirportIATA(String airportIATA) {
        this.airportIATA = airportIATA;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
