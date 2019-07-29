import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes
from pyflink.table.descriptors import Kafka, Json, Schema, Rowtime
from pyflink.table.window import Over


def over_window_agg_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = os.getcwd() + "/../result/table_over_window_agg_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
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
        .register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").over_window(Over.partition_by("a")
                                      .order_by("rowtime").preceding("30.minutes").alias("w")) \
        .select("a, max(b) over w, min(c) over w").insert_into("result")

    st_env.execute("over window agg streaming")
    # cat table/result/table_over_window_agg_streaming.csv
    # a,1,1
    # b,2,2
    # a,3,1
    # a,4,4
    # b,4,5

    # if preceding("unbounded_ranges") e.g:
    # st_env.scan("source").over_window(Over.partition_by("a")
    #                                       .order_by("rowtime").preceding("unbounded_range").alias("w")) \
    #         .select("a, max(b) over w, min(c) over w").insert_into("result")
    # the result is
    # a,1,1
    # a,3,1
    # a,4,1
    # b,2,2
    # b,4,2
    # rows is similar to time, you can refer to the doc.


if __name__ == '__main__':
    over_window_agg_streaming()
