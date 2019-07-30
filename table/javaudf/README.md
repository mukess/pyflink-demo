# UDF
This page helps users to use udf in pyflink

## Build UDF

### Scalar Function
The example of Scalar Function lives in scalar-function. You need to build this code:

```shell
cd scalar-function; mvn clean package
```

### Table Function
The example of Scalar Function lives in scalar-function. You need to build this code:

```shell
cd table-function; mvn clean package
```

### Aggregate Function
The example of Scalar Function lives in scalar-function. You need to build this code:

```shell
cd aggregate-function; mvn clean package
```

## Run Java UDF In PyFlink

### [optional] Run In Local PVM(Python Virtual Machine)
1. put udf jar(scalar-function-1.0.jar, table-function-1.0.jar, aggregate-function-1.0.jar) in Python site-packages/pyflink/lib directory
2. use python interpreter to run the code in scalar_func_demo.py or table_func_demo.py or aggregate_func_demo.py

### [optional] Run Job In Flink Cluster
1. put the udf jars into the lib directory of build-target

2. copy the opt/flink-table-*.jar to the lib directory of build-target

```shell
cd flink/build-target; cp opt/flink-table-*.jar lib/
```
 
3. start flink cluster. You can start the standard alone flink cluster:

```shell
bin/start-cluster.sh
```

you need to cd to directory of build-target in flink source code.

4. submit the python job:

```shell
bin/flink run -py <pyflink-demo path>/table/javaudf/scalar_func_demo.py
```

```shell
bin/flink run -py <pyflink-demo path>/table/javaudf/table_func_demo.py
```

```shell
bin/flink run -py <pyflink-demo path>/table/javaudf/aggregate_func_demo.py.py
```
