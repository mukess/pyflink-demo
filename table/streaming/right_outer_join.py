from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def right_outer_join_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    st_env = StreamTableEnvironment.create(s_env)
    left = st_env.from_elements(
        [(1, "1a", "1laa"), (2, "2a", "2aa"), (3, None, "3aa"), (2, "4b", "4bb"), (5, "5a", "5aa")],
        ["a", "b", "c"]).select("a, b, c")
    right = st_env.from_elements([(1, "1b", "1bb"), (2, None, "2bb"), (1, "3b", "3bb"), (4, "4b", "4bb")],
                                 ["d", "e", "f"]).select("d, e, f")

    result = left.right_outer_join(right, "a = d").select("b, e")
    # TODO: need retract table sink
    st_env.execute("right outer join streaming")
