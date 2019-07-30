import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def in_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_in_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.where("a.in(%s)" % right)
    result.insert_into("result")
    # another way
    # bt_env.register_table("RightTable", right)
    # result = left.where("a.in(RightTable)")
    bt_env.execute("in batch")

    # cat /tmp/table_in_batch.csv
    # 1,ra,raa
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc


if __name__ == '__main__':
    in_batch()
