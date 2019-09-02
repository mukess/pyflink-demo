from pyflink.java_gateway import get_gateway
from pyflink.table import TableSink
from pyflink.table.types import _to_java_type
from pyflink.util import utils


class TestRetractSink(TableSink):
    def __init__(self, field_names, field_types):
        gateway = get_gateway()
        j_test_retract_table_sink = gateway.jvm.com.pyflink.table.TestRetractSink()
        j_field_names = utils.to_jarray(gateway.jvm.String, field_names)
        j_field_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(field_type) for field_type in field_types])
        j_test_retract_table_sink = j_test_retract_table_sink.configure(j_field_names, j_field_types)
        super(TestRetractSink, self).__init__(j_test_retract_table_sink)
