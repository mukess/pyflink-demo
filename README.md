## Build PyFlink

If you want to build a PyFlink package that can be used for pip installation, you need to build Flink jars first, as described in https://ci.apache.org/projects/flink/flink-docs-master/flinkDev/building.html

```shell
mvn clean install -DskipTests -Dfast
```

Then you need to copy the jar package flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar in the directory of flink-connectors/flink-sql-connector-kafka-0.11

```shell
cp flink-connectors/flink-sql-connector-kafka-0.11/target/flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar build-target/lib
```

Then you need to copy the jar package flink-connector-elasticsearch6_*-SNAPSHOT.jar in the directory of flink-connectors/flink-connector-elasticsearch6

```shell
cp flink-connectors/flink-connector-elasticsearch6/target/flink-connector-elasticsearch6_*-SNAPSHOT.jar build-target/lib
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

## Prepare Kafka
Some demo choose kafka as source, so you need to install and run kafka in local host. the version we use kafka_2.11-0.11 (https://archive.apache.org/dist/kafka/0.11.0.3/kafka_2.11-0.11.0.3.tgz)
you use the following command to download:

```shell
wget https://archive.apache.org/dist/kafka/0.11.0.3/kafka_2.11-0.11.0.3.tgz
```

Then you depress the tar package:

```shell
tar zxvf kafka_2.11-0.11.0.3.tgz
```
Next you start the zookeeper:

```shell
cd kafka_2.11-0.11.0.3; bin/zookeeper-server-start.sh config/zookeeper.properties
```

Finally, you start kafka server:

```shell
bin/kafka-server-start.sh config/server.properties
```

# Prepare ElasticSearch
Some demo choose Elasticsearch as sink, so you need to install and run Elasticsearch in local host. the version we use elasticsearch-6.0.1 (https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.1.tar.gz)
you use the following command to download:

```shell
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.1.tar.gz
```

Then you depress the tar package:

```shell
tar zxvf https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.0.1.tar.gz
```

Finally, you start ElasticSearch:

```shell
./bin/elasticsearch
```

## Install Dependency
Install environment dependency

```shell
pip install -r requirements.txt
```

## Run demo
1. You can use PyCharm to open the project and choose the python interpreter as the python which match the pip tool which install the pyflink and dependency in requirements.txt.
2. Demos about table api is in the table_api_example.py which is in the directory of table/table_api