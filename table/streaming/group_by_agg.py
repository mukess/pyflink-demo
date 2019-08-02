import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Elasticsearch, Schema, Json


def group_by_agg_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    # use blink table planner
    st_env = StreamTableEnvironment.create(s_env, environment_settings=EnvironmentSettings.new_instance()
                                           .in_streaming_mode().use_blink_planner().build())
    # use flink table planner
    # st_env = StreamTableEnvironment.create(s_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    st_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    st_env.connect(
        Elasticsearch()
        .version("6")
        .host("localhost", 9200, "http")
        .index("group_by_agg_streaming")
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
    groub_by_table = orders.group_by("a").select("a, b.sum as d")
    # Because the schema of index user in elasticsearch is
    # {"a":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
    # "b":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}
    # so we need to cast the type in our demo.
    st_env.register_table("group_table", groub_by_table)
    result = st_env.sql_query("SELECT a, CAST(d AS VARCHAR) from group_table")
    result.insert_into("result")
    st_env.execute("group by agg streaming")
    # curl -X GET 'http://localhost:9200/group_by_agg_streaming/_search'
    # {
    #     "took": 2,
    #     "timed_out": false,
    #     "_shards": {
    #         "total": 5,
    #         "successful": 5,
    #         "skipped": 0,
    #         "failed": 0
    #     },
    #     "hits": {
    #         "total": 2,
    #         "max_score": 1,
    #         "hits": [
    #             {
    #                 "_index": "group_by_agg_streaming",
    #                 "_type": "group_by_agg_streaming",
    #                 "_id": "b",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "b",
    #                     "b": "6"
    #                 }
    #             },
    #             {
    #                 "_index": "group_by_agg_streaming",
    #                 "_type": "group_by_agg_streaming",
    #                 "_id": "a",
    #                 "_score": 1,
    #                 "_source": {
    #                     "a": "a",
    #                     "b": "13"
    #                 }
    #             }
    #         ]
    #     }
    # }


if __name__ == '__main__':
    from table.prepare_environment import prepare_env
    prepare_env(need_upsert_sink=True)
    group_by_agg_streaming()
