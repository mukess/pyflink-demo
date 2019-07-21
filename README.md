## Build PyFlink

If you want to build a PyFlink package that can be used for pip installation, you need to build Flink jars first, as described in [Build Flink](##Build Flink).

```shell
mvn clean install -DskipTests -Dfast
```

Then you need to copy the jar package flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar in the directory of flink-connectors/flink-sql-connector-kafka-0.11

```shell
cp flink-connectors/flink-sql-connector-kafka-0.11/target/flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar build-target/lib
```

Next you need to copy the jar package flink-json-*-SNAPSHOT-sql-jar.jar in the directory of flink-formats/flink-json

```shell
cp flink-formats/flink-json/target/flink-json-*-SNAPSHOT-sql-jar.jar build-target/lib
```

Next go to the root directory of flink source code and run this command to build the sdist package and wheel package:

```shell
cd flink-python; python3 setup.py sdist bdist_wheel
```

The sdist and wheel package will be found under `./flink-python/dist/`. Either of them could be used for pip installation, such as:

```shell
pip install dist/*.tar.gz
```

## Install Dependency
Install kafka-python

```shell
pip install kafka-python
```