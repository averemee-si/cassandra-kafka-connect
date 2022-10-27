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

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;

/**
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraWrapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraWrapper.class);
	private static final String MANDATORY_PARAM_NOT_SET = "Mandatory parameter '%s' is not set!";

	private CqlSession session;
	private final Duration execTimeout;
	private final int schemaType;
	private final String keyspaceName;
	private final ConsistencyLevel consistencyLevel;
	private final boolean autoCreateTable;
	private final boolean upsertMode;
	private final Integer recordTtl;
	private final KafkaKeyFieldsGetter keyFieldsGetter;
	private final KafkaValueFieldsGetter valueFieldsGetter;
	private final KafkaToCassandraTableNameMapper tableNameMapper;
	private final KafkaDeleteEventQualifier deleteEventQualifier;
	private final Map<String, CassandraTable> tablesInProcessing = new HashMap<>();
	private final String connectorName;
	private final String authType;
	private final int backoffMillis;
	private final long maxMetadataWait;
	private final boolean doesTopicDefinesTable;
	private final List<String> tablesOnCassandra;

	public static final String DEFAULT_PROTOCOL_VERSION = "V4";

	public CassandraWrapper(final String connectorName, final CassandraSinkConnectorConfig config) {
		this.connectorName = connectorName;

		// Copy configuration from OS/container environment
		CassandraConfigUtils.configure();

		// Process and analyze parameters
		execTimeout = Duration.ofMillis(config.getLong(ParamConstants.EXECUTE_TIMEOUT_MS_PARAM));
		autoCreateTable = config.getBoolean(ParamConstants.AUTO_CREATE_PARAM);
		upsertMode = StringUtils.equals(
				config.getString(ParamConstants.WRITE_MODE_PARAM), ParamConstants.WRITE_MODE_UPSERT);
		recordTtl = (config.getInt(ParamConstants.TTL_PARAM) == 0) ? null : config.getInt(ParamConstants.TTL_PARAM);

		schemaType = config.getSchemaType();

		keyFieldsGetter = config.getKeyFieldsGetter();
		valueFieldsGetter = config.getValueFieldsGetter();
		tableNameMapper = config.getTableNameMapper();
		deleteEventQualifier = config.getDeleteEventQualifier();
		backoffMillis = config.getInt(ParamConstants.BACKOFF_MILLIS_PARAM);
		maxMetadataWait = config.getLong(ParamConstants.METADATA_WAIT_PARAM);

		switch (config.getString(ParamConstants.CONSISTENCY_PARAM)) {
		case ParamConstants.CONSISTENCY_ANY:
			consistencyLevel = ConsistencyLevel.ANY;
			break;
		case ParamConstants.CONSISTENCY_ONE:
			consistencyLevel = ConsistencyLevel.ONE;
			break;
		case ParamConstants.CONSISTENCY_TWO:
			consistencyLevel = ConsistencyLevel.TWO;
			break;
		case ParamConstants.CONSISTENCY_THREE:
			consistencyLevel = ConsistencyLevel.THREE;
			break;
		case ParamConstants.CONSISTENCY_QUORUM:
			consistencyLevel = ConsistencyLevel.QUORUM;
			break;
		case ParamConstants.CONSISTENCY_ALL:
			consistencyLevel = ConsistencyLevel.ALL;
			break;
		case ParamConstants.CONSISTENCY_EACH_QUORUM:
			consistencyLevel = ConsistencyLevel.EACH_QUORUM;
			break;
		case ParamConstants.CONSISTENCY_SERIAL:
			consistencyLevel = ConsistencyLevel.SERIAL;
			break;
		case ParamConstants.CONSISTENCY_LOCAL_SERIAL:
			consistencyLevel = ConsistencyLevel.LOCAL_SERIAL;
			break;
		case ParamConstants.CONSISTENCY_LOCAL_ONE:
			consistencyLevel = ConsistencyLevel.LOCAL_ONE;
			break;
		default:
			consistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
			break;
		}

		authType = config.getString(ParamConstants.SECURITY_PARAM);
		CqlSessionBuilder builder = CqlSession.builder();
		if (StringUtils.equals(authType, ParamConstants.SECURITY_ASTRA_DB)) {
			// Check Astra DB parameters
			if (StringUtils.isBlank(config.getString(ParamConstants.ASTRA_DB_CONNECT_BUNDLE_PARAM))) {
				final String message = String.format(MANDATORY_PARAM_NOT_SET, ParamConstants.ASTRA_DB_CONNECT_BUNDLE_PARAM);
				LOGGER.error(message);
				throw new ConnectException(message);
			}
			if (StringUtils.isBlank(config.getString(ParamConstants.ASTRA_DB_CLIENT_ID_PARAM))) {
				final String message = String.format(MANDATORY_PARAM_NOT_SET, ParamConstants.ASTRA_DB_CLIENT_ID_PARAM);
				LOGGER.error(message);
				throw new ConnectException(message);
			}
			if (StringUtils.isBlank(config.getPassword(ParamConstants.ASTRA_DB_CLIENT_SECRET_PARAM).value())) {
				final String message = String.format(MANDATORY_PARAM_NOT_SET, ParamConstants.ASTRA_DB_CLIENT_SECRET_PARAM);
				LOGGER.error(message);
				throw new ConnectException(message);
			}
			builder = builder
					.withCloudSecureConnectBundle(Paths.get(config.getString(ParamConstants.ASTRA_DB_CONNECT_BUNDLE_PARAM)))
					.withAuthCredentials(
							config.getString(ParamConstants.ASTRA_DB_CLIENT_ID_PARAM),
							config.getPassword(ParamConstants.ASTRA_DB_CLIENT_SECRET_PARAM).value());
		} else {
			final int port;
			if (StringUtils.equals(authType, ParamConstants.SECURITY_AWS_PASSWORD) ||
					StringUtils.equals(authType, ParamConstants.SECURITY_AWS_SIGV4)) {
				port = ParamConstants.PORT_AWS_DEFAULT;
				CassandraConfigUtils.configure4Aws(config);
			} else {
				port = config.getInt(ParamConstants.CONTACT_POINTS_PORT_PARAM);
			}

			if (StringUtils.isBlank(config.getString(ParamConstants.CONTACT_POINTS_PARAM))) {
				final String message = String.format(MANDATORY_PARAM_NOT_SET, ParamConstants.CONTACT_POINTS_PARAM);
				LOGGER.error(message);
				throw new ConnectException(message);
			}
			for (String hostAddress : config.getString(ParamConstants.CONTACT_POINTS_PARAM).split(",")) {
				LOGGER.debug("Initializing {} using contact points='{}', port='{}'",
						CassandraWrapper.class.getCanonicalName(), hostAddress, port);
				builder = builder.addContactPoint(new InetSocketAddress(hostAddress, port));
			}

			if (StringUtils.isBlank(config.getString(ParamConstants.LOCAL_DATACENTER_PARAM))) {
				final String message = String.format(MANDATORY_PARAM_NOT_SET, ParamConstants.LOCAL_DATACENTER_PARAM);
				LOGGER.error(message);
				throw new ConnectException(message);
			}
			LOGGER.debug("Adding localDC {}", config.getString(ParamConstants.LOCAL_DATACENTER_PARAM));
			builder = builder.withLocalDatacenter(config.getString(ParamConstants.LOCAL_DATACENTER_PARAM));

			if (StringUtils.equals(authType, ParamConstants.SECURITY_PASSWORD)) {
				//Plaintext auth outside AWS
				builder = builder.withAuthCredentials(
						config.getString(ParamConstants.USERNAME_PARAM),
						config.getPassword(ParamConstants.PASSWORD_PARAM).value());
			}
			//TODO
			//TODO - Kerberos
			//TODO
		}


		final CqlSession supportSession = builder.build();
		final boolean amazonKeyspaces = StringUtils.equals(
				supportSession.getMetadata().getClusterName().get(), ParamConstants.AWS_CLUSTER_NAME);
		final String ksNameCheck = config.getString(ParamConstants.KEYSPACE_PARAM);
		if (amazonKeyspaces) {
			if ('\"' == ksNameCheck.charAt(0) && '\"' == ksNameCheck.charAt(ksNameCheck.length() - 1)) {
				keyspaceName = ksNameCheck;
			} else {
				keyspaceName = "\"" + ksNameCheck + "\"";
			}
		} else {
			if ('\"' == ksNameCheck.charAt(0) && '\"' == ksNameCheck.charAt(ksNameCheck.length() - 1)) {
				keyspaceName = StringUtils.substring(ksNameCheck, 1, ksNameCheck.length() - 1);
			} else {
				keyspaceName = StringUtils.lowerCase(ksNameCheck);
			}
		}
		doesTopicDefinesTable = StringUtils.equals(ParamConstants.DRIVING_SIDE_KAFKA,
				config.getString(ParamConstants.DRIVING_SIDE_PARAM));
		if (doesTopicDefinesTable) {
			tablesOnCassandra = null;
		} else {
			tablesOnCassandra = new ArrayList<>();
			for (final String tableName : config.getList(ParamConstants.TABLES_PARAM)) {
				if (amazonKeyspaces) {
					if ('\"' == tableName.charAt(0) && '\"' == tableName.charAt(tableName.length() - 1)) {
						tablesOnCassandra.add(tableName);
					} else {
						tablesOnCassandra.add("\"" + tableName + "\"");
					}
				} else {
					if ('\"' == tableName.charAt(0) && '\"' == tableName.charAt(tableName.length() - 1)) {
						tablesOnCassandra.add(StringUtils.substring(tableName, 1, tableName.length() - 1));
					} else {
						tablesOnCassandra.add(StringUtils.lowerCase(tableName));
					}
				}
			}
		}

		if (!supportSession.getMetadata().getKeyspace(keyspaceName).isPresent()) {
			if (config.getBoolean(ParamConstants.KEYSPACE_CREATE_ENABLED_PARAM)) {
				try {
					final int numReplicas = config.getInt(ParamConstants.KEYSPACE_CREATE_NUM_REPLICAS_PARAM);
					LOGGER.info("Keyspace '{}' will be created with {} replicas",
							keyspaceName, numReplicas);
					final CreateKeyspace createKeyspace = SchemaBuilder
							.createKeyspace(keyspaceName)
							.withSimpleStrategy(numReplicas);
					supportSession.execute(createKeyspace.build());
					LOGGER.info("Keyspace '{}' created using {} for number of replicas.", keyspaceName, numReplicas);
				} catch (Exception e) {
					LOGGER.error("Unable to create KEYSPACE '{}'!", keyspaceName);
					throw new ConnectException(e);
				}
				// Amazon Keyspaces specific...
				// Keyspace creation with metadata back propagation takes up to 20 seconds
				boolean waitForKeyspace = true;
				long totalWait = 0;
				do {
					try {
						supportSession
							.getMetadata()
							.getKeyspace(keyspaceName).get();
						waitForKeyspace = false;
					} catch (NoSuchElementException nseDummy) {}
					if (waitForKeyspace) {
						try {
							Thread.sleep(backoffMillis);
							totalWait += backoffMillis;
							if (totalWait > maxMetadataWait) {
								throw new ConnectException("Wait for metadata for KEYSPACE " +
										keyspaceName + " exceeded " + 
										ParamConstants.METADATA_WAIT_PARAM + "=" + maxMetadataWait + "!");
							}
							LOGGER.debug("Waiting {}ms for KEYSPACE '{}' creation...",
									backoffMillis, keyspaceName);
						} catch (InterruptedException ieDummy) {}
					}
				} while (waitForKeyspace);
				LOGGER.debug("Waited {} ms for KEYSPACE '{}' creation.",
						totalWait, keyspaceName);
			} else {
				LOGGER.error("Keyspace '{}' must exist before running connector!", keyspaceName);
				LOGGER.error("Or you can set parameter '{}' to true and parameter '{}' to appropriate value (default = 1) for automatic keyspace creation by connector!",
						ParamConstants.KEYSPACE_CREATE_ENABLED_PARAM, ParamConstants.KEYSPACE_CREATE_NUM_REPLICAS_PARAM);
				throw new ConnectException("Keyspace not exist!");
			}
		}
		supportSession.close();

		builder = builder.withKeyspace(keyspaceName);
		session = builder.build();

		final Metadata metadata =  session.getMetadata();
		LOGGER.info("Connected to cluster {} with available nodes:", metadata.getClusterName().get());
		metadata.getNodes().forEach((uuid, node) -> 
			LOGGER.info("\tNode ID='{}', DC='{}', Rack='{}', address='{}', Cassandra Version='{}'",
					uuid, node.getDatacenter(), node.getRack(),
					node.getBroadcastRpcAddress().get(), node.getCassandraVersion().toString())
		);
		final DriverExecutionProfile defaultProfile = session.getContext().getConfig().getDefaultProfile();
		LOGGER.info("Using protocol version {}", 
				defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION, DEFAULT_PROTOCOL_VERSION));

	}



	public void destroy() {
		if (session != null) {
			session.close();
		}
	}

	public void processSinkRecord(final SinkRecord record) {
		final List<String> currentTables;
		if (doesTopicDefinesTable) {
			currentTables = Collections.singletonList(tableNameMapper.getTableName(record, schemaType));
		} else {
			currentTables = tablesOnCassandra;
		}
		final boolean isDeleteOp = deleteEventQualifier.isDeleteEvent(record, schemaType);
		for (final String tableName : currentTables) {
			CassandraTable tableDef = tablesInProcessing.get(tableName);
			if (tableDef == null) {
				if (isDeleteOp) {
					if (!tableExist(tableName)) {
						LOGGER.warn("Unable to perform DELETE opertaion over non-existent table!");
						LOGGER.warn("Sink record content:\n\tTopic={}, partition={}, offset={}",
								record.topic(), record.kafkaPartition(), record.kafkaOffset());
						return;
					} else {
						tableDef = createTableDef(tableName, record);
						tablesInProcessing.put(tableName, tableDef);
					}
				} else {
					tableDef = createTableDef(tableName, record);
					tablesInProcessing.put(tableName, tableDef);
				}
			}
			tableDef.processSinkRecord(isDeleteOp, record);
		}
	}

	private CassandraTable createTableDef(final String tableName, final SinkRecord record) {
		if (!tableExist(tableName)) {
			if (autoCreateTable) {
				Object createRequest = SchemaBuilder
						.createTable(keyspaceName, tableName);
				// From Kafka we are unable to separate primary key to partition key and clustering key
				// so... just generic primary key...
				boolean firstColumn = true;
				final List<Field> keyFields = keyFieldsGetter.getKeyFields(record, schemaType);
				final List<Field> valueFields = valueFieldsGetter.getValueFields(record, schemaType, keyFields);

				for (Field key : keyFields) {
					final String columnName = key.name();
					final DataType dataType = CassandraColumn.dataTypeFromKafkaSchema(key.schema());
					LOGGER.debug("Adding to table '{}.{}' PK column '{}' with data type '{}'",
							keyspaceName, tableName, columnName, dataType);
					if (firstColumn) {
						firstColumn = false;
						createRequest = ((CreateTableStart)createRequest)
								.withPartitionKey(columnName, dataType);
					} else {
						createRequest = ((CreateTable)createRequest)
								.withPartitionKey(columnName, dataType);
					}
				}
				for (Field value : valueFields) {
					final String columnName = value.name();
					final DataType dataType = CassandraColumn.dataTypeFromKafkaSchema(value.schema());
					LOGGER.debug("Adding to table '{}.{}' column '{}' with data type '{}'",
							keyspaceName, tableName, columnName, dataType);
					createRequest = ((CreateTable)createRequest)
							.withColumn(columnName, dataType);
				}
				try {
					session.execute(((CreateTable)createRequest).build());
				} catch (Exception e) {
					LOGGER.error("Unable to execute '{}'!", ((CreateTable)createRequest).asCql());
					throw new ConnectException(e);
				}
				// Amazon Keyspaces specific...
				// Table creation with metadata back propagation takes up to 20 seconds
				boolean waitForTable = true;
				long totalWait = 0;
				do {
					try {
						session
							.getMetadata()
							.getKeyspace(keyspaceName).get()
							.getTable(tableName).get();
						waitForTable = false;
					} catch (NoSuchElementException nseDummy) {}
					if (waitForTable) {
						try {
							Thread.sleep(backoffMillis);
							totalWait += backoffMillis;
							if (totalWait > maxMetadataWait) {
								throw new ConnectException("Wait for metadata for table " +
										keyspaceName + "." + tableName + " exceeded " + 
										ParamConstants.METADATA_WAIT_PARAM + "=" + maxMetadataWait + "!");
							}
							LOGGER.debug("Waiting {}ms for table '{}.{}' creation...",
									backoffMillis, keyspaceName, tableName);
						} catch (InterruptedException ieDummy) {}
					}
				} while (waitForTable);
				LOGGER.debug("Waited {} ms for '{}.{}' creation.",
						totalWait, keyspaceName, tableName);

			} else {
				LOGGER.error("Table '{}.{}' must exist before running connector when {}=false!",
						keyspaceName, tableName, ParamConstants.AUTO_CREATE_PARAM);
				throw new ConnectException("Unable to create table " + keyspaceName + "." + tableName + "!");
			}
		}

		CassandraTable table = new CassandraTable(this, tableName, record);
		return table;
	}

	protected boolean tableExist(final String tableName) {
		return session
				.getMetadata()
				.getKeyspace(keyspaceName)
				.flatMap(ksmd -> ksmd.getTable(tableName))
				.isPresent();
	}

	protected CqlSession cqlSession() {
		return session;
	}

	protected String keyspace() {
		return keyspaceName;
	}

	protected ConsistencyLevel consistency() {
		return consistencyLevel;
	}

	protected boolean upsert() {
		return upsertMode;
	}

	protected Duration timeout() {
		return execTimeout;
	}

	protected int schema() {
		return schemaType;
	}

	protected Integer ttl() {
		return recordTtl;
	}

	protected String connector() {
		return connectorName;
	}

	public void dropKeyspace(final String keyspaceName) {
		Drop dropKeyspace = SchemaBuilder
				.dropKeyspace(keyspaceName)
				.ifExists();
		try {
			session.execute(dropKeyspace.build());
		} catch (Exception e) {
			LOGGER.error("Unable to execute '{}'!", dropKeyspace.asCql());
			throw e;
		}
	}

}
