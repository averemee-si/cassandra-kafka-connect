# a2-cassandra-kafka-connect
The Cassandra Sink Connector allows you to ingest data to [Apache Cassandra](https://cassandra.apache.org/), [ScyllaDB](https://scylladb.com/), and [Amazon Keyspaces](https://aws.amazon.com/keyspaces/) tables from [Apache Kafka](http://kafka.apache.org/) topic. Optimized to work with [Amazon Keyspaces](https://aws.amazon.com/keyspaces/): all required dependencies including [Starfield digital certificate](https://docs.aws.amazon.com/keyspaces/latest/devguide/using_java_driver.html#using_java_driver.BeforeYouBegin) are already included in bundle, AWS SigV4 authentication can be enabled by configuring a connector parameter. Optimized for container environment: instead of creating **application.conf** you can pass DataStax Java Driver configuration using environment variables (detailed in **DataStax Java Driver configuration** section below).
Example configurations cassandra-sink-cassandra-driven.properties & cassandra-sink-topic-driven.properties are included in bundle.

## Distribution
1. [GitHub](https://github.com/averemee-si/cassandra-kafka-connect)

## Getting Started
These instructions will get you a copy of the project up and running on any platform with JDK8+ support.

### Prerequisites

Before using **a2-cassandra-kafka-connect** please check that required Java11+ is installed with

```
echo "Checking Java version"
java -version
```

### Installing

Build without integration tests, i.e. without [Docker](https://www.docker.com/) installed)

```
mvn clean install -DskipITs
```
For installation with integration test (requires [Docker](https://www.docker.com/))

```
mvn clean install
```


## Running

**a2-cassandra-kafka-connect** supports different formats of messages in Kafka topic using parameter **a2.schema.type**. Default is **a2.schema.type=key_value** to maintain compatibility with our flagship connector [oracdc](https://github.com/averemee-si/oracdc). For consuming messages from topic created by [Confluent JDBC Source Connector](https://docs.confluent.io/kafka-connect-jdbc/current/source-connector/index.html) please set **a2.schema.type=plain**. Also please see description of parameter **a2.schema.type** below.

**a2-cassandra-kafka-connect** can operate in two different modes. When **a2.driving.side** is set to **kafka** (default) **a2-cassandra-kafka-connect** is working only with one table in Cassandra which name is determined by calling helper method of class which name is specified by **a2.tablename.mapper** parameter. When default implementation is used for **a2.tablename.mapper** and **a2.schema.type=key_value|plain** this is name of Kafka topic. In this mode, when **a2.autocreate** is set **true**, **a2-cassandra-kafka-connect** can create missed Cassandra table using schema information from Kafka topic. When **a2.driving.side** is set to **cassandra** you must set the value for **a2.tables** parameter. All tables with names present in **a2.tables** must exist in Cassandra before running **a2-cassandra-kafka-connect**. **a2.driving.side=cassandra** is very useful when topic contains information from relational database table and you need to "fan-out" this information to many Cassandra tables according to [Chebotko diagrams](https://www.researchgate.net/publication/308856170_A_Big_Data_Modeling_Methodology_for_Apache_Cassandra). For example you have **EMP** topic which contains messages representing **EMP** table from famous [Oracle RDBMS SCOTT schema](https://www.orafaq.com/wiki/SCOTT). You need to replicate this information to Cassandra and query Cassandra data using **EMPNO**, **MGR** and **DEPTNO**. To achieve this you need to create in Cassandra three tables:

```
CREATE TABLE "CHEBOTKO"."EMP"(
	"EMPNO" smallint,
	"ENAME" text,
	"JOB" text,
	"MGR" smallint,
	"HIREDATE" date,
	"SAL" decimal,
	"COMM" decimal,
	"DEPTNO" tinyint,
	PRIMARY KEY("EMPNO"))
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}, 
	'point_in_time_recovery':{
		'status':'enabled'
	}, 
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
};

CREATE TABLE "CHEBOTKO"."EMP_BY_MGR"(
	"MGR" smallint,
	"EMPNO" smallint,
	PRIMARY KEY(("MGR"), "EMPNO"))
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}, 
	'point_in_time_recovery':{
		'status':'enabled'
	}, 
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND CLUSTERING ORDER BY("EMPNO" ASC);

CREATE TABLE "CHEBOTKO"."EMP_BY_DEPT"(
	"DEPTNO" tinyint,
	"EMPNO" smallint,
	PRIMARY KEY(("DEPTNO"), "EMPNO"))
WITH CUSTOM_PROPERTIES = {
	'capacity_mode':{
		'throughput_mode':'PAY_PER_REQUEST'
	}, 
	'point_in_time_recovery':{
		'status':'enabled'
	}, 
	'encryption_specification':{
		'encryption_type':'AWS_OWNED_KMS_KEY'
	}
} AND CLUSTERING ORDER BY("EMPNO" ASC);

```
and set **a2.tables** to **EMP,EMP_BY_MGR,EMP_BY_DEPT**


### a2-cassandra-kafka-connect Connector's parameters

`a2.contact.points` - Comma separated list of Cassandra/ScyllaDB hosts or Amazon Keyspaces service endpoint (cassandra.<region-code>.amazonaws.com) to connect to. it is recommended to specify at least two hosts for achieving high availability

`a2.contact.points.port` - Specifies the port that the Cassandra/ScyllaDB hosts are listening on. Use **9142** for Amazon Keyspaces. Default - **9042**

`a2.local.datacenter` - Specifies the local Data Center name that is local to the machine on which the connector is running. Use AWS Region code when connecting to Amazon Keyspaces, otherwise run the following CQL against a contact point to find it:

```
SELECT data_center FROM system.local;
```

`a2.keyspace` - Name of keyspace to use

`a2.security` - The authentication protocol to use against Cassandra/ScyllaDB/Amazon Keyspaces. Supports plain text password authentication, Kerberos, or no authentication for Cassandra and ScyllaDB. Plaintext with service specific credentials and SigV4 are supported for Amazon Keyspaces. Valid values - **NONE**, **PASSWORD**, **KERBEROS**, **AWS_PASSWORD**, **AWS_SIGV4**. Default value - **NONE**

`a2.security.username` - The username to connect to Cassandra/ScyllaDB/Amazon Keyspaces when `a2.security` is set to **PASSWORD** or **AWS_PASSWORD**

`a2.security.password` - The password to connect to Cassandra/ScyllaDB/Amazon Keyspaces when `a2.security` is set to **PASSWORD** or **AWS_PASSWORD**

`a2.consistency` - The requested consistency level to use when writing to Cassandra/ScyllaDB/Amazon Keyspaces. The Consistency Level determines how many replicas in a cluster that must acknowledge read or write operations before it is considered successful. Valid values: **ANY**, **ONE**, **TWO**, **THREE**, **QUORUM**, **ALL**, **LOCAL_QUORUM**, **EACH_QUORUM**, **SERIAL**, **LOCAL_SERIAL**, **LOCAL_ONE**. Default - **LOCAL_QUORUM**

`a2.execute.timeout.ms` - The timeout for executing a Cassandra/ScyllaDB/Amazon Keyspaces statement. **30000** - default value

`a2.schema.type` - Type of schema used by connector: **key_value** - struct with KEY and VALUE separate (default), **plain** - STRUCT with VALUE only, **debezium** - Debezium source compatible

`a2.driving.side` - If set to **kafka** (the default), the connector determines the table name from information in the Kafka topic using the class specified by the `a2.tablename.mapper` parameter. If set to **cassandra**, the connector reads the table names using the value of the `a2.tables` parameter. This allows you to write information from one Kafka message to several Cassandra tables at once

`a2.tables` - List of tables to write. Must be set when `a2.driving.side`=**cassandra**

`a2.write.mode` - The type of statement to build when writing to a Cassandra/ScyllaDB/Amazon Keyspaces: **upsert** or **insert**. Default - **upsert**

`a2.ttl` - The retention period (seconds) for the data in Cassandra/ScyllaDB/Amazon Keyspaces. If this configuration is not provided, the Connector will perform insert operations in Cassandra/ScyllaDB/Amazon Keyspaces without the TTL setting

`a2.autocreate` - Automatically create the destination table if missed. Default - **false**

`a2.tablename.mapper` - The fully-qualified class name of the class that computes name of Cassandra table based on information from Kafka topic. The default **solutions.a2.kafka.cassandra.DefaultKafkaToCassandraTableNameMapper** uses topic name as Cassandra table name when `a2.schema.type` set to **plain** or **key_value** and value of source.table field when a2.schema.type set to **debezium**. To override this default you need to create a class that implements the **solutions.a2.kafka.cassandra.KafkaToCassandraTableNameMapper** interface

`a2.kafka.key.fields.getter` - The fully-qualified class name of the class that extracts list of key fields definitions from Kafka SinkRecord. The default **solutions.a2.kafka.cassandra.DefaultKafkaKeyFieldsGetter** just returns fields of keySchema() when `a2.schema.type` set to **key_value** or **debezium**, and list of all non-optional fields when `a2.schema.type` set to **plain**. To override this default you need to create a class that implements the **solutions.a2.kafka.cassandra.KafkaKeyFieldsGetter** interface

`a2.kafka.value.fields.getter` - The fully-qualified class name of the class that extracts list of value fields definitions from Kafka SinkRecord. The default **solutions.a2.kafka.cassandra.DefaultKafkaValueFieldsGetter** just returns fields of valueSchema() when `a2.schema.type` set to **key_value** or **debezium**, and list of all optional fields when `a2.schema.type` set to **plain**. To override this default you need to create a class that implements the **solutions.a2.kafka.cassandra.KafkaValueFieldsGetter** interface

`a2.kafka.delete.event.qualifier` - The fully-qualified class name of the class that determines that source operation was deletion. The default **solutions.a2.kafka.cassandra.DefaultKafkaDeleteEventQualifier** returns true if SinkRecord header **'op'** set to **'d'** or **'D'**, or when `a2.schema.type` set to **debezium** when **'op'** field set to **'d'** or always false when `a2.schema.type` set to **plain**. To override this default you need to create a class that implements the **solutions.a2.kafka.cassandra.KafkaDeleteEventQualifier** interface

`a2.backoff.ms` - Backoff interval in ms. Default - **200**

`a2.metadata.timeout.ms` - Max interval in ms to wait for table metadata availability. Default - **60000**

`a2.keyspace.create.enabled` - Flag to determine if the keyspace should be created if it does not exist. Default - **false**

`a2.keyspace.create.num.replicas` - Specifies the replication factor to use if a keyspace is created by the connector. Default - **1**

`a2.offset.flush.batch` - Specifies the number of Sink records before call of SinkTask.flush(). Default - **100**


### Monitoring
**a2-cassandra-kafka-connect** publishes a number of metrics about the connector’s activities that can be monitored through JMX. For complete list of metrics please refer to [JMX-METRICS.md](doc/JMX-METRICS.md)

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

* Support for [Astra DB](https://www.datastax.com/products/datastax-astra)
* Timezone shift parameter for date/time datatypes
* Kerberos auth

## Version history

####1.0.0 (AUG-2022)

Initial release


## Authors

* **Aleksej Veremeev** - *Initial work* - [A2 Rešitve d.o.o.](https://a2-solutions.eu/)

## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

