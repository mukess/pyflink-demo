import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def order_by_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = os.getcwd() + "/../result/table_order_by_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)

    left = bt_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.order_by("a.asc")
    result.insert_into("result")
    bt_env.execute("order by batch")

    # cat table/result/table_order_by_batch.csv
    # 1,ra,raa
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc
    # 4,ra,raa


if __name__ == '__main__':
    order_by_batch()
