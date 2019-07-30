package com.pyflink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";

    public void eval(String str) {
        for (String s : str.split(separator)) {
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}
