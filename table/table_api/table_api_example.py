from pyflink.table import *
from pyflink.datastream import *
from pyflink.dataset import *
from pyflink.table.window import *
from pyflink.table.descriptors import *
from utils import kafka_utils
import os

# b_env = ExecutionEnvironment.get_execution_environment()
# b_env.set_parallelism(1)
# bt_env = BatchTableEnvironment.create(b_env)
# s_env = StreamExecutionEnvironment.get_execution_environment()
# s_env.set_parallelism(1)
# st_env = StreamTableEnvironment.create(s_env)
source_file = '/tmp/table_orders.csv'


def scan_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_scan_batch.csv"
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
    orders.insert_into("result")
    bt_env.execute("scan batch")
    # cat /tmp/table_scan_batch.csv
    # a,1,1,2013-01-01 00:14:13.0
    # b,2,2,2013-01-01 00:24:13.0
    # a,3,3,2013-01-01 00:34:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0
    # a,5,2,2013-01-01 01:34:13.0


def scan_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_scan_streaming.csv"
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
    orders.insert_into("result")
    st_env.execute("scan streaming")
    # cat /tmp/table_scan_streaming.csv
    # a,1,1,2013-01-01 00:14:13.0
    # b,2,2,2013-01-01 00:24:13.0
    # a,3,3,2013-01-01 00:34:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0
    # a,5,2,2013-01-01 01:34:13.0


def select_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_select_batch.csv"
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
    # cat /tmp/table_select_batch.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


def select_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_select_streaming.csv"
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

    # cat /tmp/table_select_streaming.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


def alias_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_alias_batch.csv"
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
    # cat /tmp/table_alias_batch.csv
    # a,1,1,2013-01-01 00:14:13.0
    # b,2,2,2013-01-01 00:24:13.0
    # a,3,3,2013-01-01 00:34:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0
    # a,5,2,2013-01-01 01:34:13.0


def alias_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_alias_streaming.csv"
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
    result = orders.alias("x, y, z, t").select("x, y, z, t")
    result.insert_into("result")
    st_env.execute("alias streaming")
    # cat /tmp/table_alias_streaming.csv
    # a,1,1,2013-01-01 00:14:13.0
    # b,2,2,2013-01-01 00:24:13.0
    # a,3,3,2013-01-01 00:34:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0
    # a,5,2,2013-01-01 01:34:13.0


def where_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_where_batch.csv"
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
    result = orders.where("a === 'b'")
    result.insert_into("result")
    bt_env.execute("where batch")
    # cat table_where_batch.csv
    # b,2,2,2013-01-01 00:24:13.0
    # b,4,5,2013-01-01 01:24:13.0


def where_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
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
    # cat table_where_streaming.csv
    # b,2,2,2013-01-01 00:24:13.0
    # b,4,5,2013-01-01 01:24:13.0


def filter_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_filter_batch.csv"
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
    result = orders.filter("b % 2 === 0")
    result.insert_into("result")
    bt_env.execute("filter batch")
    # cat table_filter_batch.csv
    # b,2,2,2013-01-01 00:24:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0


def filter_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_filter_streaming.csv"
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
    result = orders.filter("b % 2 === 0")
    result.insert_into("result")
    st_env.execute("filter streaming")
    # cat table_filter_streaming.csv
    # b,2,2,2013-01-01 00:24:13.0
    # a,4,4,2013-01-01 01:14:13.0
    # b,4,5,2013-01-01 01:24:13.0


def add_columns_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_add_columns_batch.csv"
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
                               CsvTableSink(["a", "b", "c", "rowtime", "d"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.STRING()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.add_columns("concat(a, '_sunny') as d")
    result.insert_into("result")
    bt_env.execute("add columns batch")
    # cat table_add_columns_batch.csv
    # a,1,1,2013-01-01 00:14:13.0,a_sunny
    # b,2,2,2013-01-01 00:24:13.0,b_sunny
    # a,3,3,2013-01-01 00:34:13.0,a_sunny
    # a,4,4,2013-01-01 01:14:13.0,a_sunny
    # b,4,5,2013-01-01 01:24:13.0,b_sunny
    # a,5,2,2013-01-01 01:34:13.0,a_sunny


def add_columns_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_add_columns_streaming.csv"
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
                               CsvTableSink(["a", "b", "c", "rowtime", "d"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.STRING()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.add_columns("concat(a, '_sunny') as d")
    result.insert_into("result")
    st_env.execute("add columns streaming")
    # cat table_add_columns_streaming.csv
    # a,1,1,2013-01-01 00:14:13.0,a_sunny
    # b,2,2,2013-01-01 00:24:13.0,b_sunny
    # a,3,3,2013-01-01 00:34:13.0,a_sunny
    # a,4,4,2013-01-01 01:14:13.0,a_sunny
    # b,4,5,2013-01-01 01:24:13.0,b_sunny
    # a,5,2,2013-01-01 01:34:13.0,a_sunny


def add_or_replace_columns_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_add_or_replace_columns_batch.csv"
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
    result = orders.add_or_replace_columns("concat(a, '_sunny') as a")
    result.insert_into("result")
    bt_env.execute("add or replace columns batch")
    # cat table_add_or_replace_columns_batch.csv
    # a_sunny,1,1,2013-01-01 00:14:13.0
    # b_sunny,2,2,2013-01-01 00:24:13.0
    # a_sunny,3,3,2013-01-01 00:34:13.0
    # a_sunny,4,4,2013-01-01 01:14:13.0
    # b_sunny,4,5,2013-01-01 01:24:13.0
    # a_sunny,5,2,2013-01-01 01:34:13.0


def add_or_replace_columns_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_add_or_replace_columns_streaming.csv"
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
    result = orders.add_or_replace_columns("concat(a, '_sunny') as a")
    result.insert_into("result")
    st_env.execute("add or replace columns streaming")
    # cat table_add_or_replace_columns_streaming.csv
    # a_sunny,1,1,2013-01-01 00:14:13.0
    # b_sunny,2,2,2013-01-01 00:24:13.0
    # a_sunny,3,3,2013-01-01 00:34:13.0
    # a_sunny,4,4,2013-01-01 01:14:13.0
    # b_sunny,4,5,2013-01-01 01:24:13.0
    # a_sunny,5,2,2013-01-01 01:34:13.0


def drop_columns_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_drop_columns_batch.csv"
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
                               CsvTableSink(["a", "b", "rowtime"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.drop_columns("c")
    result.insert_into("result")
    bt_env.execute("drop columns batch")
    # cat /tmp/table_drop_columns_batch.csv
    # a,1,2013-01-01 00:14:13.0
    # b,2,2013-01-01 00:24:13.0
    # a,3,2013-01-01 00:34:13.0
    # a,4,2013-01-01 01:14:13.0
    # b,4,2013-01-01 01:24:13.0
    # a,5,2013-01-01 01:34:13.0


def drop_columns_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_drop_columns_streaming.csv"
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
                               CsvTableSink(["a", "b", "rowtime"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.drop_columns("c")
    result.insert_into("result")
    st_env.execute("drop columns streaming")
    # cat /tmp/table_drop_columns_streaming.csv
    # a,1,2013-01-01 00:14:13.0
    # b,2,2013-01-01 00:24:13.0
    # a,3,2013-01-01 00:34:13.0
    # a,4,2013-01-01 01:14:13.0
    # b,4,2013-01-01 01:24:13.0
    # a,5,2013-01-01 01:34:13.0


def rename_columns_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_rename_columns_batch.csv"
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
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.rename_columns("a as a2, b as b2").select("a2, b2")
    result.insert_into("result")
    bt_env.execute("rename columns batch")
    # cat /tmp/table_rename_columns_batch.csv
    # a,1
    # b,2
    # a,3
    # a,4
    # b,4
    # a,5


def rename_columns_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
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


def group_by_agg_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_group_by_agg_batch.csv"
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
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.group_by("a").select("a, b.sum as d")
    result.insert_into("result")
    bt_env.execute("group by agg batch")
    # cat /tmp/table_group_by_agg_batch.csv
    # a,13
    # b,6


def group_by_agg_streaming():
    # TODO:
    pass


def group_by_window_agg_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_group_by_window_agg_batch.csv"
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
                               CsvTableSink(["a", "start", "end", "rowtime", "d"],
                                            [DataTypes.STRING(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.TIMESTAMP(),
                                             DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Tumble.over("1.hours").on("rowtime").alias("w")) \
        .group_by("a, w") \
        .select("a, w.start, w.end, w.rowtime, b.sum as d")
    result.insert_into("result")
    bt_env.execute("group by agg batch")
    # cat /tmp/table_group_by_window_agg_batch.csv
    # a,2013-01-01 00:00:00.0,2013-01-01 01:00:00.0,2013-01-01 00:59:59.999,4
    # a,2013-01-01 01:00:00.0,2013-01-01 02:00:00.0,2013-01-01 01:59:59.999,9
    # b,2013-01-01 00:00:00.0,2013-01-01 01:00:00.0,2013-01-01 00:59:59.999,2
    # b,2013-01-01 01:00:00.0,2013-01-01 02:00:00.0,2013-01-01 01:59:59.999,4


def group_by_window_agg_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_group_by_window_agg_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("user")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ) \
        .with_format(  # declare a format for this system
            Json()
            .fail_on_missing_field(True)
            .json_schema(
                "{"
                "  type: 'object',"
                "  properties: {"
                "    a: {"
                "      type: 'string'"
                "    },"
                "    b: {"
                "      type: 'string'"
                "    },"
                "    c: {"
                "      type: 'string'"
                "    },"
                "    time: {"
                "      type: 'string',"
                "      format: 'date-time'"
                "    }"
                "  }"
                "}"
             )
         ) \
        .with_schema(  # declare the schema of the table
             Schema()
             .field("rowtime", DataTypes.TIMESTAMP())
             .rowtime(
                Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(60000))
             .field("a", DataTypes.STRING())
             .field("b", DataTypes.STRING())
             .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").window(Tumble.over("1.hours").on("rowtime").alias("w")) \
        .group_by("w, a") \
        .select("a, max(b)").insert_into("result")

    st_env.execute("group by window agg streaming")
    # cat /tmp/table_group_by_window_agg_streaming.csv
    # a,3
    # b,2


def over_window_agg_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_over_window_agg_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    st_env \
        .connect(  # declare the external system to connect to
            Kafka()
            .version("0.11")
            .topic("user")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
        ) \
        .with_format(  # declare a format for this system
            Json()
            .fail_on_missing_field(True)
            .json_schema(
                "{"
                "  type: 'object',"
                "  properties: {"
                "    a: {"
                "      type: 'string'"
                "    },"
                "    b: {"
                "      type: 'string'"
                "    },"
                "    c: {"
                "      type: 'string'"
                "    },"
                "    time: {"
                "      type: 'string',"
                "      format: 'date-time'"
                "    }"
                "  }"
                "}"
             )
         ) \
        .with_schema(  # declare the schema of the table
             Schema()
             .field("rowtime", DataTypes.TIMESTAMP())
             .rowtime(
                Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(60000))
             .field("a", DataTypes.STRING())
             .field("b", DataTypes.STRING())
             .field("c", DataTypes.STRING())
         ) \
        .in_append_mode() \
        .register_table_source("source")

    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    st_env.scan("source").over_window(Over.partition_by("a")
                                      .order_by("rowtime").preceding("30.minutes").alias("w")) \
        .select("a, max(b) over w, min(c) over w").insert_into("result")

    st_env.execute("over window agg streaming")
    # cat /tmp/table_over_window_agg_streaming.csv
    # a,1,1
    # b,2,2
    # a,3,1
    # a,4,4
    # b,4,5

    # if preceding("unbounded_ranges") e.g:
    # st_env.scan("source").over_window(Over.partition_by("a")
    #                                       .order_by("rowtime").preceding("unbounded_range").alias("w")) \
    #         .select("a, max(b) over w, min(c) over w").insert_into("result")
    # the result is
    # a,1,1
    # a,3,1
    # a,4,1
    # b,2,2
    # b,4,2
    # rows is similar to time, you can refer to the doc.


# TODO:distinct agg

def distinct_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_distinct_batch.csv"
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
    result = orders.select("b").distinct()
    result.insert_into("result")
    bt_env.execute("distinct batch")
    # cat /tmp/table_distinct_batch.csv
    # 1
    # 2
    # 3
    # 4
    # 5


def distinct_streaming():
    # TODO:
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_distinct_streaming.csv"
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
                               CsvTableSink(["b"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = st_env.scan("Orders")
    result = orders.select("b").distinct()
    result.insert_into("result")
    st_env.execute("distinct streaming")


def inner_join_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_inner_join_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.join(right).where("a = d").select("a, b, e")
    result.insert_into("result")
    bt_env.execute("inner join batch")
    # cat /tmp/table_inner_join_batch.csv
    # 1,1a,1b
    # 2,2a,
    # 2,4b,
    # 1,1a,3b


def inner_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_inner_join_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.join(right).where("a = d").select("a, b, e")
    result.insert_into("result")
    st_env.execute("inner join streaming")
    # cat /tmp/table_inner_join_streaming.csv
    # 1,1a,1b
    # 2,4b,
    # 2,2a,
    # 1,1a,3b


def left_outer_join_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_left_outer_join_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.left_outer_join(right, "a = d").select("a, b, e")
    result.insert_into("result")
    bt_env.execute("left outer join batch")
    # cat /tmp/table_left_outer_join_batch.csv
    # 1,1a,1b
    # 1,1a,3b
    # 2,2a,
    # 2,4b,
    # 3,,
    # 5,5a,


def left_outer_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")

    result = left.left_outer_join(right, "a = d").select("a, b, e")
    # TODO: sink
    st_env.execute("left outer join streaming")


def right_outer_join_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_right_outer_join_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.right_outer_join(right, "a = d").select("a, b, e")
    result.insert_into("result")
    bt_env.execute("right outer join batch")
    # cat /tmp/table_right_outer_join_batch.csv
    # 1,1a,1b
    # 1,1a,3b
    # 2,2a,
    # 2,4b,
    # 4b


def right_outer_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")

    result = left.right_outer_join(right, "a = d").select("a, b, e")
    # TODO: sink
    st_env.execute("right outer join streaming")


def full_outer_join_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_full_outer_join_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.full_outer_join(right, "a = d").select("a, b, e")
    result.insert_into("result")
    bt_env.execute("full outer join batch")
    # cat /tmp/table_full_outer_join_batch.csv
    # 1,1a,1b
    # 1,1a,3b
    # 2,2a,
    # 2,4b,
    # 3,,
    # 5,5a,
    # 4b


def full_outer_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")

    result = left.full_outer_join(right, "a = d").select("a, b, e")
    result.insert_into("result")
    # TODO: sink
    st_env.execute("full outer join streaming")


def union_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_union_batch.csv"
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

    result = left.union(right)
    result.insert_into("result")
    bt_env.execute("union batch")
    # cat /tmp/table_union_batch.csv
    # 1,1a,1laa
    # 1,1b,1bb
    # 1,3b,3bb
    # 2,,2bb
    # 2,2a,2aa
    # 3,,3aa
    # 4,4b,4bb
    # note : Unions two tables with duplicate records removed whatever the duplicate record from
    # the same table or the other.


def union_all_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_union_all_batch.csv"
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


def union_all_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_union_all_streaming.csv"
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
    # cat /tmp/table_union_all_streaming.csv
    # 1,1b,1bb
    # 2,,2bb
    # 1,3b,3bb
    # 4,4b,4bb
    # 1,1a,1laa
    # 2,2a,2aa
    # 3,,3aa
    # 1,1a,1laa
    # 1,1b,1bb


def intersect_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_intersect_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements([(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (1, "ra", "raa")],
                                ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.intersect(right)
    result.insert_into("result")
    bt_env.execute("intersect batch")
    # cat /tmp/table_intersect_batch.csv
    # 1,ra,raa


def intersect_all_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_intersect_all_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements([(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (1, "ra", "raa")],
                                ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.intersect_all(right)
    result.insert_into("result")
    bt_env.execute("intersect all batch")
    # cat /tmp/table_intersect_all_batch.csv
    # 1,ra,raa
    # 1,ra,raa


def minus_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_minus_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (1, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.minus(right)
    result.insert_into("result")
    bt_env.execute("minus batch")
    # cat /tmp/table_minus_batch.csv
    # 2,lb,lbb
    # 3,,lcc


def minus_all_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_minus_all_batch.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = bt_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (1, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = bt_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a, b, c")
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file))

    result = left.minus_all(right)
    result.insert_into("result")
    bt_env.execute("minus all batch")
    # cat /tmp/table_minus_all_batch.csv
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc


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


def in_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    result_file = "/tmp/table_in_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    left = st_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a")

    result = left.where("a.in(%s)" % right)
    # another way
    # st_env.register_table("RightTable", right)
    # result = left.where("a.in(RightTable)")
    # TODO: sink
    st_env.execute("in streaming")


def order_by_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_order_by_batch.csv"
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

    # cat /tmp/table_order_by_batch.csv
    # 1,ra,raa
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc
    # 4,ra,raa


def offset_and_fetch_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file_1 = "/tmp/table_offset_and_fetch_batch_1.csv"
    result_file_2 = "/tmp/table_offset_and_fetch_batch_2.csv"
    result_file_3 = "/tmp/table_offset_and_fetch_batch_3.csv"
    if os.path.exists(result_file_1):
        os.remove(result_file_1)
    if os.path.exists(result_file_2):
        os.remove(result_file_2)
    if os.path.exists(result_file_3):
        os.remove(result_file_3)

    bt_env.register_table_sink("result1",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file_1))

    bt_env.register_table_sink("result2",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file_2))

    bt_env.register_table_sink("result3",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.BIGINT(),
                                             DataTypes.STRING(),
                                             DataTypes.STRING()],
                                            result_file_3))

    left = bt_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")

    ordered_table = left.order_by("a.asc")

    ordered_table.fetch(5).insert_into("result1")
    ordered_table.offset(1).insert_into("result2")
    ordered_table.offset(1).fetch(2).insert_into("result3")

    bt_env.execute("offset and fetch batch")
    # cat /tmp/table_offset_and_fetch_batch_1.csv
    # 1,ra,raa
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc
    # 4,ra,raa

    # cat /tmp/table_offset_and_fetch_batch_2.csv
    # 2,lb,lbb
    # 2,lb,lbb
    # 3,,lcc
    # 4,ra,raa

    # cat /tmp/table_offset_and_fetch_batch_3.csv
    # 2,lb,lbb
    # 2,lb,lbb


def tumble_row_window_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_tumble_row_window_batch.csv"
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
                               CsvTableSink(["a"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Tumble.over("2.rows").on("rowtime").alias("w")) \
        .group_by("w, a").select("b.sum")
    result.insert_into("result")
    bt_env.execute("tumble row window batch")
    # cat /tmp/table_tumble_row_window_batch.csv
    # 4
    # 9
    # 6


def slide_time_window_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_slide_time_window_batch.csv"
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
                               CsvTableSink(["a"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Slide.over("60.minutes").every("10.minutes").on("rowtime").alias("w")) \
        .group_by("w").select("b.sum")
    result.insert_into("result")
    bt_env.execute("slide time window batch")
    # cat /tmp/table_slide_time_window_batch.csv
    # 1
    # 3
    # 6
    # 6
    # 6
    # 6
    # 9
    # 11
    # 13
    # 13
    # 13
    # 13
    # 9
    # 5


def session_time_window_batch():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    result_file = "/tmp/table_session_time_window_batch.csv"
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
                               CsvTableSink(["a"],
                                            [DataTypes.INT()],
                                            result_file))
    orders = bt_env.scan("Orders")
    result = orders.window(Session.with_gap("10.minutes").on("rowtime").alias("w")) \
        .group_by("w").select("b.sum")
    result.insert_into("result")
    bt_env.execute("session time window batch")
    # cat /tmp/table_session_time_window_batch.csv
    # 6
    # 13


def prepare_environment():
    if os.path.exists(source_file):
        os.remove(source_file)
    with open(source_file, 'w') as f:
        f.write("a,1,1,2013-01-01 00:14:13\n")
        f.write("b,2,2,2013-01-01 00:24:13\n")
        f.write("a,3,3,2013-01-01 00:34:13\n")
        f.write("a,4,4,2013-01-01 01:14:13\n")
        f.write("b,4,5,2013-01-01 01:24:13\n")
        f.write("a,5,2,2013-01-01 01:34:13")
        f.close()

    topics = kafka_utils.list_topics()
    if 'user' not in topics:
        kafka_utils.create_topic('user')
        msgs = [{'a': 'a', 'b': 1, 'c': 1, 'time': '2013-01-01T00:14:13Z'},
                {'a': 'b', 'b': 2, 'c': 2, 'time': '2013-01-01T00:24:13Z'},
                {'a': 'a', 'b': 3, 'c': 3, 'time': '2013-01-01T00:34:13Z'},
                {'a': 'a', 'b': 4, 'c': 4, 'time': '2013-01-01T01:14:13Z'},
                {'a': 'b', 'b': 4, 'c': 5, 'time': '2013-01-01T01:24:13Z'},
                {'a': 'a', 'b': 5, 'c': 2, 'time': '2013-01-01T01:34:13Z'}]
        for msg in msgs:
            kafka_utils.send_msg('user', msg)


if __name__ == '__main__':
    # prepare_environment()
    # scan_batch()
    # scan_streaming()
    # select_batch()
    # select_streaming()
    # alias_batch()
    # alias_streaming()
    # where_batch()
    # where_streaming()
    # filter_batch()
    # filter_streaming()
    # add_columns_batch()
    # add_columns_streaming()
    # add_or_replace_columns_batch()
    # add_or_replace_columns_streaming()
    # drop_columns_batch()
    # drop_columns_streaming()
    # rename_columns_batch()
    # rename_columns_streaming()
    # group_by_agg_batch()
    # group_by_window_agg_batch()
    over_window_agg_streaming()
    # distinct_batch()
    # inner_join_batch()
    # inner_join_streaming()
    # left_outer_join_batch()
    # left_outer_join_streaming()
    # right_outer_join_batch()
    # right_outer_join_streaming()
    # full_outer_join_batch()
    # full_outer_join_streaming()
    # union_batch()
    # union_all_batch()
    # union_all_streaming()
    # intersect_batch()
    # intersect_all_batch()
    # minus_batch()
    # minus_all_batch()
    # in_batch()
    # in_streaming()
    # order_by_batch()
    # offset_and_fetch_batch()
    # tumble_row_window_batch()
    # slide_time_window_batch()
    # session_time_window_batch()
