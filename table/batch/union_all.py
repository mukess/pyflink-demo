import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def union_all_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = os.getcwd() + "/tmp/table_union_all_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (1, "1a", "1laa"), (1, "1b", "1bb")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.union_all(right)
    result.insert_into("result")
    bt_env.execute("union all batch")
    # cat /tmp/table_union_all_batch.csv
    # 1,1a,1laa
    # 2,2a,2aa
    # 3,,3aa
    # 1,1a,1laa
    # 1,1b,1bb
    # 1,1b,1bb
    # 2,,2bb
    # 1,3b,3bb
    # 4,4b,4bb


if __name__ == '__main__':
    union_all_batch()
