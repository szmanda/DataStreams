package com.example.bigdata.model;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

public class CombinedDelay implements Serializable, AggregateFunction<CombinedDelay, CombinedDelay, CombinedDelay> {

    private Integer delay = 0;
    private String infoType;
    private String airport;
    private String state;
    private Date date;

    private Integer timeZone;

    @Override
    public String toString() {
        return "CombinedDelay{" +
                "delay='" + delay.toString() + '\'' +
                ", infoType='" + infoType + '\'' +
//                ", airport='" + airport + '\'' +
                ", state='" + state + '\'' +
                ", date='"+ date + "'" +
                ", timezone='"+ timeZone +"'"+
                ", utcDate='"+ getUtcDate() +"'" +
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

    public Date getUtcDate() {
        if (date == null) return null;
//        System.out.println(date);
//        System.out.println(timeZone);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, -timeZone);
        return calendar.getTime();
    }

    @Override
    public CombinedDelay createAccumulator() { return new CombinedDelay(); }
    @Override
    public CombinedDelay add(CombinedDelay newCD, CombinedDelay aggCD) { return merge(newCD, aggCD); }
    @Override
    public CombinedDelay getResult(CombinedDelay combinedDelay) { return combinedDelay; }
    @Override
    public CombinedDelay merge(CombinedDelay a, CombinedDelay b) {
        if (b == null) b = new CombinedDelay();
//        System.out.println("merging: "+a.toString()+"with: "+b.toString()+"\n");
        a.setDelay(a.getDelay() + b.getDelay());
        return a;
    }
}
