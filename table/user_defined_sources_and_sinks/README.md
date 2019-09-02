# User-defined Sources & Sinks
This page helps users to custom create sources & sinks

## Build Sources & Sinks

### Custom Sink
The example of custom restract table sink lives in sinks module. You need to build this code:

```shell
cd sinks; mvn clean package
```

1.put jar(source or sink jar) into Python site-packages/pyflink/lib directory
2.create your python code wrapped the java class(you can refer to TestRetractSink.py)

