package com.pyflink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class TestRetractSink implements RetractStreamTableSink<Row> {

    String[] fNames;
    TypeInformation<?>[] fTypes;

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fTypes, fNames);
    }

    @Override
    public String[] getFieldNames() {
        return fNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fTypes;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.addSink(new RowSink());
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fNames, TypeInformation<?>[] fTypes) {
        TestRetractSink copy = new TestRetractSink();
        copy.fNames = fNames;
        copy.fTypes = fTypes;
        return copy;
    }

    private static class RowSink implements SinkFunction<Tuple2<Boolean, Row>> {
        @Override
        public void invoke(Tuple2<Boolean, Row> value) throws Exception {
            System.out.println(value);
        }
    }
}

