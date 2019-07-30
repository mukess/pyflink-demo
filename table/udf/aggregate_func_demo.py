import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


def aggregate_func_python_table_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_table = bt_env.from_elements([("a", 1, 1), ("a", 2, 2), ("b", 3, 2), ("a", 5, 2)],
                                        ["user", "points", "level"])

    result_file = "/tmp/aggregate_func_python_table_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.BIGINT()],
                                            result_file))
    bt_env.register_java_function("wAvg", "com.pyflink.table.WeightedAvg")
    result = source_table.group_by("user").select("user, wAvg(points, level) as avgPoints")
    result.insert_into("result")
    bt_env.execute("aggregate func python table api")
    # cat  /tmp/aggregate_func_python_table_api.csv
    # a,3
    # b,3


def aggregate_func_python_sql_api():
    b_env = ExecutionEnvironment.get_execution_environment()
    b_env.set_parallelism(1)
    bt_env = BatchTableEnvironment.create(b_env)
    source_table = bt_env.from_elements([("a", 1, 1), ("a", 2, 2), ("b", 3, 2), ("a", 5, 2)],
                                        ["user", "points", "level"])

    result_file = "/tmp/aggregate_func_python_sql_api.csv"
    if os.path.exists(result_file):
        os.remove(result_file)
    bt_env.register_table_sink("result",
                               CsvTableSink(["a", "b"],
                                            [DataTypes.STRING(),
                                             DataTypes.BIGINT()],
                                            result_file))

    bt_env.register_java_function("wAvg", "com.pyflink.table.WeightedAvg")
    bt_env.register_table("userScores", source_table)
    result = bt_env.sql_query("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user")
    result.insert_into("result")
    bt_env.execute("aggregate func python sql api")
    # cat /tmp/aggregate_func_python_sql_api.csv
    # a,3
    # b,3


if __name__ == '__main__':
    aggregate_func_python_table_api()
    # aggregate_func_python_sql_api()
