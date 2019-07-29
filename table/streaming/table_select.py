import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def select_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = os.getcwd() + "/../result/table_select_streaming.csv"
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
                               CsvTableSink(["a", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.select("a, b")
    result.insert_into("result")
    st_env.execute("select streaming")

    # cat table/result/table_select_streaming.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


if __name__ == '__main__':
    select_streaming()
