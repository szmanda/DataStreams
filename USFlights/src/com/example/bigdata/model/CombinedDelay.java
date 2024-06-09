package com.example.bigdata.model;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class CombinedDelay implements Serializable {

    private Integer delay = 0;
    private String infoType;
    private String airport;
    private String state;
    private Date date;

    private Integer departureCount = 0;
    private Integer arrivalCount = 0;
    private Integer departureDelay = 0;
    private Integer arrivalDelay = 0;

    private Integer timeZone;

    @Override
    public String toString() {
        return "CombinedDelay{" +
                "state='" + state + '\'' +
                ", date='"+ date + "'" +
                ", timezone='"+ timeZone +"'"+
                ", utcDate='"+ getUtcDate() +"'" +
                ", departureCount=" + departureCount +
                ", arrivalCount=" + arrivalCount +
                ", departureDelay=" + departureDelay +
                ", arrivalDelay=" + arrivalDelay +
                '}';
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

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Integer getDelay() {
        return delay;
    }

    public void setDelay(Integer delay) {
        this.delay = delay;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Integer getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(Integer timeZone) {
        this.timeZone = timeZone;
    }

    public Integer getDepartureCount() {
        return departureCount;
    }

    public void setDepartureCount(Integer departureCount) {
        this.departureCount = departureCount;
    }

    public Integer getArrivalCount() {
        return arrivalCount;
    }

    public void setArrivalCount(Integer arrivalCount) {
        this.arrivalCount = arrivalCount;
    }

    public Integer getDepartureDelay() {
        return departureDelay;
    }

    public void setDepartureDelay(Integer departureDelay) {
        this.departureDelay = departureDelay;
    }

    public Integer getArrivalDelay() {
        return arrivalDelay;
    }

    public void setArrivalDelay(Integer arrivalDelay) {
        this.arrivalDelay = arrivalDelay;
    }

    public Date getUtcDate() {
        if (date == null) return null;
//        System.out.println(date);
//        System.out.println(timeZone);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, -timeZone);
        return calendar.getTime();
    }
}
