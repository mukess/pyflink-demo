import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def alias_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = os.getcwd() + "/../result/table_alias_batch.csv"
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
                               CsvTableSink(["a", "b", "c", "rowtime"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.alias("x, y, z, t").select("x, y, z, t")
    result.insert_into("result")
    bt_env.execute("alias batch")
    # cat table/result/table_alias_batch.csv
    # a,1,1,2013-01-01 00:14:13.0
    # b,2,2,2013-01-01 00:24:13.0
    # a,3,3,2013-01-01 00:34:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0
    # a,5,2,2013-01-01 01:34:13.0


if __name__ == '__main__':
    alias_batch()
