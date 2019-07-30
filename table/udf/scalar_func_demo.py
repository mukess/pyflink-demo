import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def scalar_func_python_table_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a", "aa"), ("b", "bb"), ("c", "cc")], ["a", "b"]).select("a, b")

    result_file = "/tmp/scalar_func_python_table_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "c"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT()],
                                            result_file))

    # register the java scalar function
    bt_env.register_java_function("hashCode", "com.pyflink.table.HashCode")

    # use the java scalar function in Python Table API
    result = source_table.select("a, a.hashCode(), hashCode(a)")
    result.insert_into("result")
    bt_env.execute("scalar func python table api")
    # cat /tmp/scalar_func_python_table_api.csv
    # a,1164,1164
    # b,1176,1176
    # c,1188,1188


def scalar_func_python_sql():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)

    source_table = bt_env.from_elements([("a", 1), ("b", 2), ("c", 3)], ["a", "b"]).select("a, b")

    result_file = "/tmp/scalar_func_python_sql.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT()],
                                            result_file))

    # register the java scalar function
    bt_env.register_java_function("hashCode", "com.pyflink.table.HashCode")

    # register the table for using in the sql query
    bt_env.register_table("MyTable", source_table)

    result = bt_env.sql_query("SELECT a, hashCode(a) FROM MyTable")
    result.insert_into("result")
    bt_env.execute("scalar func python sql")
    # cat /tmp/scalar_func_python_sql.csv
    # a,1164
    # b,1176
    # c,1188


if __name__ == '__main__':
    scalar_func_python_table_api()
    # scalar_func_python_sql()
