package com.pyflink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

public class TestSource implements StreamTableSource<Row> {

    TableSchema schema;
    TypeInformation<Row> returnType;

    public TestSource(TableSchema tableSchema, TypeInformation<Row> returnType) {
        this.schema = tableSchema;
        this.returnType = returnType;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        Row r1 = new Row(1);
        r1.setField(0, "haha");
        Row r2 = new Row(1);
        r2.setField(0, "haha");
        Row r3 = new Row(1);
        r3.setField(0, "haha");
        List<Row> data = new ArrayList<>();
        data.add(r1);
        data.add(r2);
        data.add(r3);
        return env.fromCollection(data, returnType);
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return returnType;
    }
}
