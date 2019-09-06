import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, CsvTableSink
from pyflink.table.descriptors import Schema, CustomFormatDescriptor, CustomConnectorDescriptor, Json
from pyflink.table.window import Tumble


def custom_kafka_source_demo():
    custom_connector = CustomConnectorDescriptor('kafka', 1, True) \
        .property('connector.topic', 'user') \
        .property('connector.properties.0.key', 'zookeeper.connect') \
        .property('connector.properties.0.value', 'localhost:2181') \
        .property('connector.properties.1.key', 'bootstrap.servers') \
        .property('connector.properties.1.value', 'localhost:9092') \
        .properties({'connector.version': '0.11', 'connector.startup-mode': 'earliest-offset'})

    # the key is 'format.json-schema'
    custom_format = CustomFormatDescriptor('json', 1) \
        .property('format.json-schema',
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
                  "}") \
        .properties({'format.fail-on-missing-field': 'true'})

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/custom_kafka_source_demo.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env \
        .connect(custom_connector) \
        .with_format(
            custom_format
        ) \
        .with_schema(  # declare the schema of the table
            Schema()
            .field("proctime", DataTypes.TIMESTAMP())
            .proctime()
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.STRING())
            .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").window(Tumble.over("2.rows").on("proctime").alias("w")) \
        .group_by("w, a") \
        .select("a, max(b)").insert_into("result")

    st_env.execute("custom kafka source demo")
    # cat /tmp/custom_kafka_source_demo.csv
    # a,3
    # b,4
    # a 5


def custom_test_source_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/custom_test_source_demo.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    custom_connector = CustomConnectorDescriptor('pyflink-test', 1, False)
    st_env.connect(custom_connector) \
        .with_schema(
        Schema()
            .field("a", DataTypes.STRING())
    ).register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a"],
                                            [DataTypes.STRING()],
                                            result_file))
    orders = st_env.scan("source")
    orders.insert_into("result")
    st_env.execute("custom test source demo")


def custom_test_sink_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")

    result = left.left_outer_join(right, "a = d").select("a, b, e")
    # use custom retract sink connector
    custom_connector = CustomConnectorDescriptor('pyflink-test', 1, False)
    st_env.connect(custom_connector) \
        .with_schema(
        Schema()
            .field("a", DataTypes.BIGINT())
            .field("b", DataTypes.STRING())
            .field("c", DataTypes.STRING())
    ).register_table_sink("sink")
    result.insert_into("sink")
    st_env.execute("custom test sink demo")
    # (true, 1, 1a, null)
    # (true, 2, 2a, null)
    # (true, 3, null, null)
    # (true, 2, 4b, null)
    # (true, 5, 5a, null)
    # (false, 1, 1a, null)
    # (true, 1, 1a, 1b)
    # (false, 2, 4b, null)
    # (true, 2, 4b, null)
    # (false, 2, 2a, null)
    # (true, 2, 2a, null)
    # (true, 1, 1a, 3b)


if __name__ == '__main__':
    # from table.prepare_environment import prepare_env
    # prepare_env(need_stream_source=True)
    # custom_kafka_source_demo()
    # custom_test_sink_demo()
    custom_test_source_demo()
