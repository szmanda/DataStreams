package com.example.bigdata.transformations;

import com.example.bigdata.model.CombinedDelay;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;


public class DelayAggregate implements AggregateFunction<CombinedDelay, CombinedDelay, CombinedDelay> {

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
