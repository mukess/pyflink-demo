import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


# DISTINCT window aggregates are currently not supported in Batch mode.
def distinct_agg_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_distinct_agg_batch.csv"
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
                               CsvTableSink(["b"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.group_by("a") \
        .select("b.sum.distinct as d")
    result.insert_into("result")
    bt_env.execute("distinct agg batch")
    # cat table/result/table_distinct_batch.csv
    # 13
    # 6


if __name__ == '__main__':
    distinct_agg_batch()
