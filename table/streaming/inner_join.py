import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes


def inner_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_inner_join_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.join(right).where("a = d").select("a, b, e")
    result.insert_into("result")
    st_env.execute("inner join streaming")
    # cat /tmp/table_inner_join_streaming.csv
    # 1,1a,1b
    # 2,4b,
    # 2,2a,
    # 1,1a,3b


if __name__ == '__main__':
    inner_join_streaming()
