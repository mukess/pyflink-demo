from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import CustomConnectorDescriptor, Schema, Kafka, Json, Rowtime
from pyflink.table.window import Tumble


def pv_uv_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)
    # use blink table planner
    st_env = StreamTableEnvironment.create(s_env, environment_settings=EnvironmentSettings.new_instance()
                                           .in_streaming_mode().use_blink_planner().build())
    # use flink table planner
    # st_env = StreamTableEnvironment.create(s_env)
    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("user_behavior")
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
                "    user_id: {"
                "      type: 'string'"
                "    },"
                "    item_id: {"
                "      type: 'string'"
                "    },"
                "    category_id: {"
                "      type: 'string'"
                "    },"
                "    behavior: {"
                "      type: 'string'"
                "    },"
                "    ts: {"
                "      type: 'string',"
                "      format: 'date-time'"
                "    }"
                "  }"
                "}"
            )
        ) \
        .with_schema(  # declare the schema of the table
            Schema()
            .field("user_id", DataTypes.STRING())
            .field("item_id", DataTypes.STRING())
            .field("category_id", DataTypes.STRING())
            .field("behavior", DataTypes.STRING())
            .field("rowtime", DataTypes.TIMESTAMP())
            .rowtime(
                Rowtime()
                .timestamps_from_field("ts")
                .watermarks_periodic_bounded(60000))
         ) \
        .in_append_mode() \
        .register_table_source("source")

    # use custom retract sink connector
    custom_connector = CustomConnectorDescriptor('jdbc', 1, False) \
        .property("connector.driver", "org.apache.derby.jdbc.ClientDriver") \
        .property("connector.url", "jdbc:derby://localhost:1527/firstdb") \
        .property("connector.table", "pv_uv_table") \
        .property("connector.write.flush.max-rows", "1")
    st_env.connect(custom_connector) \
        .with_schema(
        Schema()
            .field("pv", DataTypes.BIGINT())
            .field("uv", DataTypes.BIGINT())
    ).register_table_sink("sink")

    st_env.scan("source").window(Tumble.over("1.hours").on("rowtime").alias("w")) \
        .group_by("w") \
        .select("COUNT(1) as pv, user_id.count.distinct as uv").insert_into("sink")

    st_env.execute("table pv uv")


if __name__ == '__main__':
    pv_uv_demo()
