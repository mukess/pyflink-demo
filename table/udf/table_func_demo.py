import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def table_func_python_table_join_lateral_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a aa aaa", "aa"), ("b bb bbb", "bb"), ("c cc ccc", "cc")],
                                        ["a", "b"]).select("a, b")

    result_file = "/tmp/table_func_python_table_join_lateral_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))

    bt_env.register_java_function("split", "com.pyflink.table.Split")

    result = source_table.join_lateral("Split(a) as (word, length)").select("a, word, length")

    result.insert_into("result")

    bt_env.execute("table func python table join lateral api")
    # cat /tmp/table_func_python_table_join_lateral_api.csv
    # a aa aaa,a,1
    # a aa aaa,aa,2
    # a aa aaa,aaa,3
    # b bb bbb,b,1
    # b bb bbb,bb,2
    # b bb bbb,bbb,3
    # c cc ccc,c,1
    # c cc ccc,cc,2
    # c cc ccc,ccc,3


def table_func_python_table_left_outer_join_lateral_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a aa aaa", "aa"), ("b bb bbb", "bb"), ("c cc ccc", "cc")],
                                        ["a", "b"]).select("a, b")

    result_file = "/tmp/table_func_python_table_left_outer_join_lateral_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))

    bt_env.register_java_function("split", "com.pyflink.table.Split")

    result = source_table.left_outer_join_lateral("Split(a) as (word, length)").select("a, word, length")

    result.insert_into("result")

    bt_env.execute("table func python table left outer join lateral api")
    # cat /tmp/table_func_python_table_left_outer_join_lateral_api.csv
    # a aa aaa,a,1
    # a aa aaa,aa,2
    # a aa aaa,aaa,3
    # b bb bbb,b,1
    # b bb bbb,bb,2
    # b bb bbb,bbb,3
    # c cc ccc,c,1
    # c cc ccc,cc,2
    # c cc ccc,ccc,3


def table_func_python_sql_join_lateral_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a aa aaa", "aa"), ("b bb bbb", "bb"), ("c cc ccc", "cc")],
                                        ["a", "b"]).select("a, b")

    result_file = "/tmp/table_func_python_sql_join_lateral_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))

    bt_env.register_java_function("split", "com.pyflink.table.Split")
    bt_env.register_table("MyTable", source_table)

    result = bt_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")

    result.insert_into("result")

    bt_env.execute("table func python sql join lateral api")
    # cat /tmp/table_func_python_sql_join_lateral_api.csv
    # a aa aaa,a,1
    # a aa aaa,aa,2
    # a aa aaa,aaa,3
    # b bb bbb,b,1
    # b bb bbb,bb,2
    # b bb bbb,bbb,3
    # c cc ccc,c,1
    # c cc ccc,cc,2
    # c cc ccc,ccc,3


def table_func_python_sql_left_outer_join_lateral_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a aa aaa", "aa"), ("b bb bbb", "bb"), ("c cc ccc", "cc")],
                                        ["a", "b"]).select("a, b")

    result_file = "/tmp/table_func_python_sql_left_outer_join_lateral_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))

    bt_env.register_java_function("split", "com.pyflink.table.Split")
    bt_env.register_table("MyTable", source_table)

    result = bt_env.sql_query(
        "SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

    result.insert_into("result")

    bt_env.execute("table func python sql left outer join lateral api")
    # cat /tmp/table_func_python_sql_left_outer_join_lateral_api.csv
    # a aa aaa,a,1
    # a aa aaa,aa,2
    # a aa aaa,aaa,3
    # b bb bbb,b,1
    # b bb bbb,bb,2
    # b bb bbb,bbb,3
    # c cc ccc,c,1
    # c cc ccc,cc,2
    # c cc ccc,ccc,3


if __name__ == '__main__':
    table_func_python_table_join_lateral_api()
    # table_func_python_table_left_outer_join_lateral_api()
    # table_func_python_sql_join_lateral_api()
    # table_func_python_sql_left_outer_join_lateral_api()
