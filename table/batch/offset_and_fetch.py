import os

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, CsvTableSink, DataTypes


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
    # cat /tmp/able_offset_and_fetch_batch_1.csv
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


if __name__ == '__main__':
    offset_and_fetch_batch()
