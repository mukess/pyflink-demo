import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def rename_columns_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_rename_columns_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env.register_table_source("Orders",
                                 CsvTableSource(source_file,
                                                ["a", "b", "c", "rowtime"],
                                                [DataTypes.STRING(),
                                                 DataTypes.INT(),
                                                 DataTypes.INT(),
                                                 DataTypes.TIMESTAMP()]))
    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.rename_columns("a as a2, b as b2").select("a2, b2")
    result.insert_into("result")
    st_env.execute("rename columns streaming")
    # cat /tmp/table_rename_columns_streaming.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


if __name__ == '__main__':
    rename_columns_streaming()
