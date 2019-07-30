# pyflink-demo
This project is to help users easier to write their pyflink job.

**contents**

- [Quick Start](#quick-start)
  + [Setup](#setup)
    + [Requirements](#requirements)
    + [Install python2](#install-python2)
    + [Install pip](#install-pip)
    + [Install java 8](#install-java-8)
    + [Install maven](#install-maven)
  + [Build PyFlink](#build-pyflink)
  + [Prepare Kafka](#prepare-kafka)
  + [Prepare ElasticSearch](#prepare-elasticsearch)
  + [Install Dependency](#install-dependency)
  + [Run Demo](#run-demo)
    + [[optional] Importing the project on PyCharm](#optionalimporting-the-project-on-pycharm)
    + [Run pyflink table api example](#run-pyflink-table-api-example)

## Quick Start

### Setup

#### Requirements
1. python2.7 or python3
2. pip
3. java 1.8
4. maven version >=3.3.0

#### Install python2

macOS
```shell
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
export PATH="/usr/local/bin:/usr/local/sbin:$PATH"
brew install python@2 
```
Ubuntu
```shell
sudo apt install python-dev
```

#### Install pip

macOS

```shell 
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

Ubuntu
```shell
sudo apt install python-pip
```

#### Install java 8

[java download page](http://www.oracle.com/technetwork/java/javase/downloads/index.html)

#### Install maven

maven version >=3.3.0

[download maven page](http://maven.apache.org/download.cgi)

```shell
tar -xvf apache-maven-3.6.1-bin.tar.gz
mv -rf apache-maven-3.6.1 /usr/local/
```
configuration environment variables
```shell
MAVEN_HOME=/usr/local/apache-maven-3.6.1
export MAVEN_HOME
export PATH=${PATH}:${MAVEN_HOME}/bin
```


### Build PyFlink

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

### Prepare Kafka
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

### Prepare ElasticSearch
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

### Install Dependency
Install environment dependency

```shell
pip install -r requirements.txt
```

### Run demo
#### [optional]Importing the project on PyCharm
You can use PyCharm to open the project and choose the python interpreter as the python which match the pip tool which install the pyflink and dependency in requirements.txt.
The following documentation describes the steps to setup PyCharm 2019.1.3 ([https://www.jetbrains.com/pycharm/download/](https://www.jetbrains.com/pycharm/download/))

If you are in the PyCharm startup interface:
1. Start PyCharm and choose "Open"
2. Select the pyflink-demo cloned repository.
3. Click on System interpreter in python interpreter option(Pycharm->Preference->python interpreter).
4. Choose the python which have installed the packages of pyflink and dependencies in the requirements.txt

If you have used PyCharm to open a project:
1. Select "File -> Open"
2. Select the pyflink-demo cloned repository.
3. Click on System interpreter in python interpreter option(Pycharm->Preference->python interpreter).
4. Choose the python which have installed the packages of pyflink and dependencies in the requirements.txt
#### Run pyflink table api example 
Demos about table api is in the pyflink-demo/table/batch directory and pyflink-demo/table/streaming directory.
Demos about udf is in the pyflink-demo/table/udf
