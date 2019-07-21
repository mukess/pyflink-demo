## Build PyFlink

If you want to build a PyFlink package that can be used for pip installation, you need to build Flink jars first, as described in [Build Flink](##Build Flink).

{% highlight bash %}
mvn clean install -DskipTests -Dfast
{% endhighlight %}

Then you need to copy the jar package flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar in the directory of flink-connectors/flink-sql-connector-kafka-0.11

{% highlight bash %} 
cp flink-connectors/flink-sql-connector-kafka-0.11/target/flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar build-target/lib
{% endhighlight %}

Next you need to copy the jar package flink-json-*-SNAPSHOT-sql-jar.jar in the directory of flink-formats/flink-json

{% highlight bash %} 
cp flink-formats/flink-json/target/flink-json-*-SNAPSHOT-sql-jar.jar build-target/lib
{% endhighlight %}

Next go to the root directory of flink source code and run this command to build the sdist package and wheel package:

{% highlight bash %}
cd flink-python; python3 setup.py sdist bdist_wheel
{% endhighlight %}

The sdist and wheel package will be found under `./flink-python/dist/`. Either of them could be used for pip installation, such as:

{% highlight bash %}
pip install dist/*.tar.gz
{% endhighlight %}

## Install Dependency
1. Install kafka-python

{% highlight bash %}
pip install kafka-python
{% endhighlight %}