from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def in_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "ra", "raa"), (2, "lb", "lbb"), (3, "", "lcc"), (2, "lb", "lbb"), (4, "ra", "raa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "ra", "raa"), (2, "", "rbb"), (3, "rc", "rcc"), (1, "ra", "raa")],
                                 ["a", "b", "c"]).select("a")

    result = left.where("a.in(%s)" % right).select("b, c")
    # another way
    # st_env.register_table("RightTable", right)
    # result = left.where("a.in(RightTable)")
    # TODO: need retract table sink
    result.insert_into("result")
    st_env.execute("in streaming")


if __name__ == '__main__':
    in_streaming()
