import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSource, CsvTableSink, DataTypes


def where_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    source_file = os.getcwd() + "/../resources/table_orders.csv"
    result_file = "/tmp/table_where_streaming.csv"
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
                               CsvTableSink(["a", "b", "c", "rowtime"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.where("a === 'b'")
    result.insert_into("result")
    st_env.execute("where streaming")
    # cat /tmp/table_where_streaming.csv
    # b,2,2,2013-01-01 00:24:13.0
    # b,4,5,2013-01-01 01:24:13.0


if __name__ == '__main__':
    where_streaming()
