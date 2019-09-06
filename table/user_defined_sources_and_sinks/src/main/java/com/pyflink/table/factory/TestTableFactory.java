package com.pyflink.table.factory;

import com.pyflink.table.sinks.TestRetractSink;
import com.pyflink.table.sources.TestSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

public class TestTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {
    @Override
    public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> map) {
        DescriptorProperties params = new DescriptorProperties(true);
        params.putProperties(map);
        new SchemaValidator(true, true, true).validate(params);
        TableSchema tableSchema = params.getTableSchema(SCHEMA);
        TestRetractSink sink = new TestRetractSink();
        return (StreamTableSink<Tuple2<Boolean, Row>>) sink.configure(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> map) {
        DescriptorProperties params = new DescriptorProperties(true);
        params.putProperties(map);
        new SchemaValidator(true, true, true).validate(params);
        TableSchema tableSchema = params.getTableSchema(SCHEMA);
        return new TestSource(tableSchema, new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames()));
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "pyflink-test");
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(SCHEMA + ".#." + SCHEMA_FROM);

        // time attributes
        properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
        properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);
        return properties;
    }
}
