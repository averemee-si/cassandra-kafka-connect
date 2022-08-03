/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.kafka.cassandra;

/**
 * Constants Definition for Cassandra/ScyllaDB/Amazon Keyspaces Sink Connector
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class ParamConstants {

	public static final String AWS_CLUSTER_NAME = "Amazon Keyspaces";

	public static final String CONTACT_POINTS_PARAM = "a2.contact.points";
	public static final String CONTACT_POINTS_DOC = "Comma separated list of Cassandra/ScyllaDB hosts or Amazon Keyspaces service endpoint (cassandra.<region-code>.amazonaws.com) to connect to. it is recommended to specify at least two hosts for achieving high availability";

	public static final String CONTACT_POINTS_PORT_PARAM = "a2.contact.points.port";
	public static final String CONTACT_POINTS_PORT_DOC = "Specifies the port that the Cassandra/ScyllaDB hosts are listening on. Use 9142 for Amazon Keyspaces. Default - 9042";
	public static final int PORT_DEFAULT = 9042; 
	public static final int PORT_AWS_DEFAULT = 9142; 

	public static final String LOCAL_DATACENTER_PARAM = "a2.local.datacenter";
	public static final String LOCAL_DATACENTER_DOC = "Specifies the local Data Center name that is local to the machine on which the connector is running. Use AWS Region code when connecting to Amazon Keyspaces, otherwise run the following CQL against a contact point to find it: SELECT data_center FROM system.local;";

	public static final String KEYSPACE_PARAM = "a2.keyspace";
	public static final String KEYSPACE_DOC = "Name of keyspace to use";

	public static final String SECURITY_PARAM = "a2.security";
	public static final String SECURITY_DOC = "The authentication protocol to use against Cassandra/ScyllaDB/Amazon Keyspaces. Supports plain text password authentication, Kerberos, or no authentication for Cassandra and ScyllaDB. Plaintext with service specific credentials and SigV4 are supported for Amazon Keyspaces.\n" +
													"Valid values - NONE, PASSWORD, KERBEROS, AWS_PASSWORD, AWS_SIGV4.\n" +
													"Default value - NONE.";
	public static final String SECURITY_NONE = "NONE";
	public static final String SECURITY_PASSWORD = "PASSWORD";
	public static final String SECURITY_KERBEROS = "KERBEROS";
	public static final String SECURITY_AWS_PASSWORD = "AWS_PASSWORD";
	public static final String SECURITY_AWS_SIGV4 = "AWS_SIGV4";

	public static final String USERNAME_PARAM = "a2.security.username";
	public static final String USERNAME_DOC = "The username to connect to Cassandra/ScyllaDB/Amazon Keyspaces when a2.security is set to PASSWORD or AWS_PASSWORD";

	public static final String PASSWORD_PARAM = "a2.security.password";
	public static final String PASSWORD_DOC = "The password to connect to Cassandra/ScyllaDB/Amazon Keyspaces when a2.security is set to PASSWORD or AWS_PASSWORD";

	public static final String CONSISTENCY_PARAM = "a2.consistency";
	public static final String CONSISTENCY_DOC = "The requested consistency level to use when writing to Cassandra/ScyllaDB/Amazon Keyspaces. The Consistency Level determines how many replicas in a cluster that must acknowledge read or write operations before it is considered successful.\n" +
													"Valid values: ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE.\n" +
													"Default - LOCAL_QUORUM.";
	public static final String CONSISTENCY_ANY = "ANY";
	public static final String CONSISTENCY_ONE = "ONE";
	public static final String CONSISTENCY_TWO = "TWO";
	public static final String CONSISTENCY_THREE = "THREE";
	public static final String CONSISTENCY_QUORUM = "QUORUM";
	public static final String CONSISTENCY_ALL = "ALL";
	public static final String CONSISTENCY_LOCAL_QUORUM = "LOCAL_QUORUM";
	public static final String CONSISTENCY_EACH_QUORUM = "EACH_QUORUM";
	public static final String CONSISTENCY_SERIAL = "SERIAL";
	public static final String CONSISTENCY_LOCAL_SERIAL = "LOCAL_SERIAL";
	public static final String CONSISTENCY_LOCAL_ONE = "LOCAL_ONE";

	public static final String EXECUTE_TIMEOUT_MS_PARAM = "a2.execute.timeout.ms";
	public static final String EXECUTE_TIMEOUT_MS_DOC = "The timeout for executing a Cassandra/ScyllaDB/Amazon Keyspaces statement. 30000 - default value";
	public static final long EXECUTE_TIMEOUT_MS_DEFAULT = 30000;

	public static final String SCHEMA_TYPE_PARAM = "a2.schema.type";
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by connector: key_value - struct with KEY and VALUE separate (default), plain - STRUCT with VALUE only, debezium - Debezium source compatible";
	public static final String SCHEMA_TYPE_DEBEZIUM = "debezium";
	public static final String SCHEMA_TYPE_VALUE_ONLY = "plain";
	public static final String SCHEMA_TYPE_KEY_VALUE = "key_value";
	public static final int SCHEMA_TYPE_INT_DEBEZIUM = 1;
	public static final int SCHEMA_TYPE_INT_VALUE_ONLY = 2;
	public static final int SCHEMA_TYPE_INT_KEY_VALUE = 3;

	public static final String DRIVING_SIDE_PARAM = "a2.driving.side";
	public static final String DRIVING_SIDE_DOC =
			"If set to 'kafka' (the default), the connector determines the table name from information in the Kafka topic using the class specified by the 'a2.tablename.mapper' parameter.\n" +
			"If set to 'cassandra', the connector reads the table names using the value of the 'a2.tables' parameter. This allows you to write information from one Kafka message to several Cassandra tables at once.";
	public static final String DRIVING_SIDE_KAFKA = "kafka";
	public static final String DRIVING_SIDE_CASSANDRA = "cassandra";

	public static final String TABLES_PARAM = "a2.tables";
	public static final String TABLES_DOC = "List of tables to write. Must be set when 'a2.driving.side=cassandra'";

	public static final String WRITE_MODE_PARAM = "a2.write.mode";
	public static final String WRITE_MODE_DOC = "The type of statement to build when writing to a Cassandra/ScyllaDB/Amazon Keyspaces: upsert or insert. Default - upsert";
	public static final String WRITE_MODE_UPSERT = "upsert";
	public static final String WRITE_MODE_INSERT = "insert";

	public static final String TTL_PARAM = "a2.ttl";
	public static final String TTL_DOC = "The retention period (seconds) for the data in Cassandra/ScyllaDB/Amazon Keyspaces. If this configuration is not provided, the Connector will perform insert operations in Cassandra/ScyllaDB/Amazon Keyspaces without the TTL setting.";

	public static final String AUTO_CREATE_PARAM = "a2.autocreate";
	public static final String AUTO_CREATE_DOC = "Automatically create the destination table if missed";
	public static final String AUTO_CREATE_DEFAULT = "false";

	public static final String TABLENAME_MAPPER_PARAM = "a2.tablename.mapper";
	public static final String TABLENAME_MAPPER_DOC =
			"The fully-qualified class name of the class that computes name of Cassandra table based on information from Kafka topic.\n" +
			"The default DefaultKafkaToCassandraTableNameMapper uses topic name as Cassandra table name when a2.schema.type set to 'plain' or 'key_value' and value of source.table field when a2.schema.type set to 'debezium'.\n" +
			"To override this default you need to create a class that implements the solutions.a2.kafka.cassandra.KafkaToCassandraTableNameMapper interface.";
	public static final String TABLENAME_MAPPER_DEFAULT = "solutions.a2.kafka.cassandra.DefaultKafkaToCassandraTableNameMapper";

	public static final String KAFKA_KEY_FLD_GETTER_PARAM = "a2.kafka.key.fields.getter";
	public static final String KAFKA_KEY_FLD_GETTER_DOC =
			"The fully-qualified class name of the class that extracts list of key fields definitions from Kafka SinkRecord.\n" +
			"The default DefaultKafkaKeyFieldsGetter just returns fields of keySchema() when a2.schema.type set to 'key_value' or 'debezium', and list of all non-optional fields when a2.schema.type set to 'plain'.\n" +
			"To override this default you need to create a class that implements the solutions.a2.kafka.cassandra.KafkaKeyFieldsGetter interface.";
	public static final String KAFKA_KEY_FLD_GETTER_DEFAULT = "solutions.a2.kafka.cassandra.DefaultKafkaKeyFieldsGetter";

	public static final String KAFKA_VALUE_FLD_GETTER_PARAM = "a2.kafka.value.fields.getter";
	public static final String KAFKA_VALUE_FLD_GETTER_DOC = 
			"The fully-qualified class name of the class that extracts list of value fields definitions from Kafka SinkRecord.\n" +
			"The default DefaultKafkaValueFieldsGetter just returns fields of valueSchema() when a2.schema.type set to 'key_value' or 'debezium', and list of all optional fields when a2.schema.type set to 'plain'.\n" +
			"To override this default you need to create a class that implements the solutions.a2.kafka.cassandra.KafkaValueFieldsGetter interface.";
	public static final String KAFKA_VALUE_FLD_GETTER_DEFAULT = "solutions.a2.kafka.cassandra.DefaultKafkaValueFieldsGetter";

	public static final String KAFKA_DELETE_EVENT_QUALIFIER_PARAM = "a2.kafka.delete.event.qualifier";
	public static final String KAFKA_DELETE_EVENT_QUALIFIER_DOC = "The fully-qualified class name of the class that determines that source operation was deletion.\n" +
			"The default DefaultKafkaDeleteEventQualifier returns true if SinkRecord header 'op' set to 'd' or 'D', or when a2.schema.type set to 'debezium' when 'op' field set to 'd' or always false when a2.schema.type set to 'plain'.\n" +
			"To override this default you need to create a class that implements the solutions.a2.kafka.cassandra.KafkaDeleteEventQualifier interface."; 
	public static final String KAFKA_DELETE_EVENT_QUALIFIER_DEFAULT = "solutions.a2.kafka.cassandra.DefaultKafkaDeleteEventQualifier"; 

	public static final String BACKOFF_MILLIS_PARAM = "a2.backoff.ms";
	public static final String BACKOFF_MILLIS_DOC = "Backoff interval in ms. Default - 200";
	public static final int BACKOFF_MILLIS_DEFAULT = 200;

	public static final String METADATA_WAIT_PARAM = "a2.metadata.timeout.ms";
	public static final String METADATA_WAIT_DOC = "Max interval in ms to wait for table metadata availability. Default - 60000";
	public static final long METADATA_WAIT_DEFAULT = 60000;

	public static final String KEYSPACE_CREATE_ENABLED_PARAM = "a2.keyspace.create.enabled";
	public static final String KEYSPACE_CREATE_ENABLED_DOC = "Flag to determine if the keyspace should be created if it does not exist. Default - false";

	public static final String KEYSPACE_CREATE_NUM_REPLICAS_PARAM = "a2.keyspace.create.num.replicas";
	public static final String KEYSPACE_CREATE_NUM_REPLICAS_DOC = "Specifies the replication factor to use if a keyspace is created by the connector. Default - 1";

	public static final String OFFSET_FLUSH_BATCH_PARAM = "a2.offset.flush.batch";
	public static final String OFFSET_FLUSH_BATCH_DOC = "Specifies the number of Sink records before call of SinkTask.flush(). Default - 100";
	public static final int OFFSET_FLUSH_BATCH_DEFAULT = 100;

}
