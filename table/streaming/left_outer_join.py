from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes

from table.user_defined_sources_and_sinks.TestRetractSink import TestRetractSink


def left_outer_join_streaming():
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
    sink = TestRetractSink(["a", "b", "c"],
                           [DataTypes.BIGINT(),
                            DataTypes.STRING(),
                            DataTypes.STRING()])
    st_env.register_table_sink("sink", sink)
    result.insert_into("sink")
    st_env.execute("left outer join streaming")
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
    left_outer_join_streaming()
