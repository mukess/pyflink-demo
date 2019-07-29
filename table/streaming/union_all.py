import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes


def union_all_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = os.getcwd() + "/../result/table_union_all_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (1, "1a", "1laa"), (1, "1b", "1bb")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["a", "b", "c"]).select("a, b, c")
    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.union_all(right)
    result.insert_into("result")
    st_env.execute("union all streaming")
    # cat table/result/table_union_all_streaming.csv
    # 1,1b,1bb
    # 2,,2bb
    # 1,3b,3bb
    # 4,4b,4bb
    # 1,1a,1laa
    # 2,2a,2aa
    # 3,,3aa
    # 1,1a,1laa
    # 1,1b,1bb


if __name__ == '__main__':
    union_all_streaming()
