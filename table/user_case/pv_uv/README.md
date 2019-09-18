# pv_uv_demo
This demo is to help users to use pyflink api to develop a pv/uv demo

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
  + [Prepare Derby](#prepare-derby)
  + [Install Dependency](#install-dependency)
  + [Prepare Data](#prepare-data)
  + [Run Demo](#run-the-demo)
    + [See the result](#see-the-result)

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

Then you need to copy the jar package flink-jdbc_*-SNAPSHOT.jar in the directory of flink-connectors/flink-jdbc

```shell
cp flink-connectors/flink-jdbc/target/flink-jdbc_*-SNAPSHOT.jar build-target/lib
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

### Prepare Derby
the pv_uv_demo need a upsert sink connector and I choose the derby, so you need to install and run Derby in local host. the version we use db-derby-10.14.2.0-lib (http://apache.mirrors.pair.com//db/derby/db-derby-10.14.2.0/db-derby-10.14.2.0-lib.tar.gz)
you use the following command to download:

```shell
wget http://apache.mirrors.pair.com//db/derby/db-derby-10.14.2.0/db-derby-10.14.2.0-lib.tar.gz
```

Then you depress the tar package:

```shell
tar zxvf http://apache.mirrors.pair.com//db/derby/db-derby-10.14.2.0/db-derby-10.14.2.0-lib.tar.gz
```

Next, you start Derby Server:

```shell
./bin/startNetworkServer -h 0.0.0.0
```

Next, you can run the ij in another terminal:

```shell
./bin/ij
```

Next, you can connect to the server in the ij interactive command:

```shell
ij> connect 'jdbc:derby://localhost:1527/firstdb;create=true';
```

Next, you need to create the result table pv_uv_table in the ij terminal:

```shell
ij> create table pv_uv_table(startTime TIMESTAMP,endTime TIMESTAMP,pv bigint,uv bigint);
```

Finally, you need to put the derby.jar, derbyclient.jar and derbytools.jar in db-derby-10.14.2.0-bin/lib into the Python directory of site-package/pyflink/lib

### Install Dependency
Install environment dependency

```shell
pip install -r requirements.txt
```

### Prepare Data
First you need to replace the variable KAFKA_DIR in file env.sh with your installed KAFKA binary directory, for example in my env.sh:

```shell
KAFKA_DIR=/Users/duanchen/Applications/kafka_2.11-0.11.0.3
```

Next, you need to source the create_data.sh

```shell
source create_data.sh
```

Next, you can start kafka

```shell
start_kafka
```

Next, you can create the topic which will be used in our demo

```shell
create_kafka_topic 1 1 user_behavior
```

Finally, you can send message to to the topic user_behavior

```shell
send_message user_behavior user_behavior.log
```

## Run The Demo
The demo code in pv-uv_example.py, you can directly run the code

### See the result
you can see the result in the ij terminal:

```shell
ij> select * from pv_uv_table;
STARTTIME                    |ENDTIME                      |PV                  |UV
-----------------------------------------------------------------------------------------------------
2017-11-26 01:00:00.0        |2017-11-26 02:00:00.0        |47244               |30837
2017-11-26 02:00:00.0        |2017-11-26 03:00:00.0        |53902               |35261
2017-11-26 03:00:00.0        |2017-11-26 04:00:00.0        |53135               |35302
2017-11-26 04:00:00.0        |2017-11-26 05:00:00.0        |49863               |33537
2017-11-26 05:00:00.0        |2017-11-26 06:00:00.0        |54305               |35748
2017-11-26 06:00:00.0        |2017-11-26 07:00:00.0        |56718               |36934
2017-11-26 07:00:00.0        |2017-11-26 08:00:00.0        |58324               |37763
2017-11-26 08:00:00.0        |2017-11-26 09:00:00.0        |58672               |37961

已选择 8 行
```
