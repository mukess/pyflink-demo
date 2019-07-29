import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def select_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = os.getcwd() + "/../result/table_select_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)

    bt_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.select("a, b")
    result.insert_into("result")
    bt_env.execute("select batch")
    # cat table/result/table_select_batch.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


if __name__ == '__main__':
    select_batch()
