from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Rowtime, Elasticsearch, Json, Kafka
from pyflink.table.window import Tumble


def distinct_agg_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)
    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("user")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ) \
        .with_format(  # declare a format for this system
            Json()
            .fail_on_missing_field(True)
            .json_schema(
                "{"
                "  type: 'object',"
                "  properties: {"
                "    a: {"
                "      type: 'string'"
                "    },"
                "    b: {"
                "      type: 'string'"
                "    },"
                "    c: {"
                "      type: 'string'"
                "    },"
                "    time: {"
                "      type: 'string',"
                "      format: 'date-time'"
                "    }"
                "  }"
                "}"
             )
         ) \
        .with_schema(  # declare the schema of the table
             Schema()
             .field("rowtime", DataTypes.TIMESTAMP())
             .rowtime(
                Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(60000))
             .field("a", DataTypes.STRING())
             .field("b", DataTypes.STRING())
             .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("Orders")
    st_env.connect(
        Elasticsearch()
        .version("6")
        .host("localhost", 9200, "http")
        .index("distinct_agg_streaming")
        .document_type('pyflink')
        .key_delimiter("_")
        .key_null_literal("null")
        .failure_handler_ignore()
        .disable_flush_on_checkpoint()
        .bulk_flush_max_actions(2)
        .bulk_flush_max_size("1 mb")
        .bulk_flush_interval(5000)
        ) \
        .with_schema(
            Schema()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.STRING())
        ) \
        .with_format(
           Json()
           .derive_schema()
        ) \
        .in_upsert_mode() \
        .register_table_sink("result")
    orders = st_env.scan("Orders")
    result = orders.window(Tumble.over("30.minutes").on("rowtime").alias("w")) \
        .group_by("a, w").select("a, b.max.distinct as d")
    result.insert_into("result")
    st_env.execute("distinct agg streaming")
    # curl -X GET 'http://localhost:9200/distinct_agg_streaming/_search'
    # {
    #     "took": 3,
    #     "timed_out": false,
    #     "_shards": {
    #         "total": 5,
    #         "successful": 5,
    #         "skipped": 0,
    #         "failed": 0
    #     },
    #     "hits": {
    #         "total": 5,
    #         "max_score": 1,
    #         "hits": [
    #             {
    #                 "_index": "distinct_agg_streaming",
    #                 "_type": "pyflink",
    #                 "_id": "3zfsHWwBHRafi3KHm2Ve",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "a",
    #                     "b": "3"
    #                 }
    #             },
    #             {
    #                 "_index": "distinct_agg_streaming",
    #                 "_type": "pyflink",
    #                 "_id": "4TfsHWwBHRafi3KHrmU-",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "b",
    #                     "b": "4"
    #                 }
    #             },
    #             {
    #                 "_index": "distinct_agg_streaming",
    #                 "_type": "pyflink",
    #                 "_id": "4DfsHWwBHRafi3KHm2Ve",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "a",
    #                     "b": "4"
    #                 }
    #             },
    #             {
    #                 "_index": "distinct_agg_streaming",
    #                 "_type": "pyflink",
    #                 "_id": "3TfsHWwBHRafi3KHm2Uf",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "a",
    #                     "b": "1"
    #                 }
    #             },
    #             {
    #                 "_index": "distinct_agg_streaming",
    #                 "_type": "pyflink",
    #                 "_id": "3jfsHWwBHRafi3KHm2Uf",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "b",
    #                     "b": "2"
    #                 }
    #             }
    #         ]
    #     }
    # }


if __name__ == '__main__':
    from table.prepare_environment import prepare_env
    prepare_env()
    distinct_agg_streaming()
