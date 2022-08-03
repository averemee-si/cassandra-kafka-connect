# cassandra-kafka-connect
The Cassandra Sink Connector allows you to ingest data to [Apache Cassandra](https://cassandra.apache.org/), [ScyllaDB](https://scylladb.com/), and [Amazon Keyspaces](https://aws.amazon.com/keyspaces/) tables from [Apache Kafka](http://kafka.apache.org/) topic.

## Distribution
1. [GitHub](https://github.com/averemee-si/cassandra-kafka-connect)

## Getting Started
These instructions will get you a copy of the project up and running on any platform with JDK8+ support.

### Prerequisites

Before using **cassandra-kafka-connect** please check that required Java11+ is installed with

```
echo "Checking Java version"
java -version
```

### Installing

Build without integration tests, i.e. without Doker installed

```
mvn clean install -DskipITs
```
For installation with integration tests

```
mvn clean install
```


## Running 

### cassandra-kafka-connect Connector's parameters
`a2.kudu.masters` - Kudu master server or a comma separated list of Kudu masters
`a2.case.sensitive.names` - Use case sensitive column names (true, default) or do not (false)
`a2.kudu.table` - Name of Kudu table
`a2.batch.size` - Maximum number of statements to include in a single batch when inserting/updating/deleting data
`a2.schema.type` - Type of schema used by **kudu-kafka-connect**: plain ([JDBC Connector](https://docs.confluent.io/kafka-connect-jdbc/current/index.html) compatible) (default) or Debezium

### Monitoring
_solutions.a2.kafka.kudu.KuduSinkConnector_ publishes a number of metrics about the connector’s activities that can be monitored through JMX. For complete list of metrics please refer to [JMX-METRICS.md](doc/JMX-METRICS.md)

### DataStax Java Driver configuration
You can add an **application.conf** file in the classpath, or you can dynamically specify configuration values with environment variables. In this case, you need to convert [TyperSafe configuration parameters used by DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/) as described below, and pass them as environment variables:

1. Convert to upper case
2. Prefix with **DATASTAX__JAVA__DRIVER_**
3. Replace a period [.] with a single underscore (_]
4. Replace a dash [-] with double underscores [__]
5. Replace an underscore [_] with triple underscores [___]

For example the following TypeSafe config

```
datastax-java-driver {
  basic.request.timeout = 5 seconds
  advanced.protocol.version = V4
}
```
can be presented as

```
export DATASTAX__JAVA__DRIVER_BASIC_REQUEST_TIMEOUT="5 seconds"
export DATASTAX__JAVA__DRIVER_ADVANCED_PROTOCOL_VERSION="V4"
```


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## TODO

* Table auto-creation from topic information

## Version history

####0.1.0 (APR-2022)

Initial release


## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](https://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

