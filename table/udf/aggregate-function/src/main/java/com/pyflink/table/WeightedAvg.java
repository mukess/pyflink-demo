package com.pyflink.table;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * Weighted Average user-defined aggregate function.
 */
public class WeightedAvg extends AggregateFunction<Long, WeightedAvg.WeightedAvgAccum> {
    @Override
    public Long getValue(WeightedAvgAccum weightedAvgAccum) {
        if (weightedAvgAccum.count == 0) {
            return null;
        } else {
            return weightedAvgAccum.sum / weightedAvgAccum.count;
        }
    }

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, long iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum other = iter.next();
            acc.count += other.count;
            acc.sum += other.sum;
        }
    }

    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0L;
    }

    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }
}
