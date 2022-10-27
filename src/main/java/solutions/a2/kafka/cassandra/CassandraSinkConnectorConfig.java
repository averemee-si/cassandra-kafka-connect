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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra/ScyllaDB/Amazon Keyspaces Sink Connector Configuration Definition
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraSinkConnectorConfig extends AbstractConfig {

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ParamConstants.CONTACT_POINTS_PARAM, Type.STRING,
						"",
						Importance.HIGH, ParamConstants.CONTACT_POINTS_DOC)
				.define(ParamConstants.CONTACT_POINTS_PORT_PARAM, Type.INT, ParamConstants.PORT_DEFAULT,
						Importance.HIGH, ParamConstants.CONTACT_POINTS_PORT_DOC)
				.define(ParamConstants.LOCAL_DATACENTER_PARAM, Type.STRING,
						"",
						Importance.HIGH, ParamConstants.LOCAL_DATACENTER_DOC)
				.define(ParamConstants.KEYSPACE_PARAM, Type.STRING,
						Importance.HIGH, ParamConstants.KEYSPACE_DOC)
				.define(ParamConstants.SECURITY_PARAM, Type.STRING,
						ParamConstants.SECURITY_NONE,
						ConfigDef.ValidString.in(
								ParamConstants.SECURITY_NONE,
								ParamConstants.SECURITY_PASSWORD,
								ParamConstants.SECURITY_KERBEROS,
								ParamConstants.SECURITY_AWS_PASSWORD,
								ParamConstants.SECURITY_AWS_SIGV4,
								ParamConstants.SECURITY_ASTRA_DB),
						Importance.HIGH, ParamConstants.SECURITY_DOC)
				.define(ParamConstants.USERNAME_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.USERNAME_DOC)
				.define(ParamConstants.PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.HIGH, ParamConstants.PASSWORD_DOC)
				.define(ParamConstants.ASTRA_DB_CONNECT_BUNDLE_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.ASTRA_DB_CONNECT_BUNDLE_DOC)
				.define(ParamConstants.ASTRA_DB_CLIENT_ID_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.ASTRA_DB_CLIENT_ID_DOC)
				.define(ParamConstants.ASTRA_DB_CLIENT_SECRET_PARAM, Type.PASSWORD, "",
						Importance.HIGH, ParamConstants.ASTRA_DB_CLIENT_SECRET_DOC)
				.define(ParamConstants.CONSISTENCY_PARAM, Type.STRING,
						ParamConstants.CONSISTENCY_LOCAL_QUORUM,
						ConfigDef.ValidString.in(
								ParamConstants.CONSISTENCY_ANY,
								ParamConstants.CONSISTENCY_ONE,
								ParamConstants.CONSISTENCY_TWO,
								ParamConstants.CONSISTENCY_THREE,
								ParamConstants.CONSISTENCY_QUORUM,
								ParamConstants.CONSISTENCY_ALL,
								ParamConstants.CONSISTENCY_LOCAL_QUORUM,
								ParamConstants.CONSISTENCY_EACH_QUORUM,
								ParamConstants.CONSISTENCY_SERIAL,
								ParamConstants.CONSISTENCY_LOCAL_SERIAL,
								ParamConstants.CONSISTENCY_LOCAL_ONE),
						Importance.HIGH, ParamConstants.CONSISTENCY_DOC)
				.define(ParamConstants.EXECUTE_TIMEOUT_MS_PARAM, Type.LONG,
						ParamConstants.EXECUTE_TIMEOUT_MS_DEFAULT,
						Importance.LOW, ParamConstants.EXECUTE_TIMEOUT_MS_DOC)
				.define(ParamConstants.SCHEMA_TYPE_PARAM, Type.STRING,
						ParamConstants.SCHEMA_TYPE_KEY_VALUE,
						ConfigDef.ValidString.in(
								ParamConstants.SCHEMA_TYPE_DEBEZIUM,
								ParamConstants.SCHEMA_TYPE_VALUE_ONLY,
								ParamConstants.SCHEMA_TYPE_KEY_VALUE),
						Importance.HIGH, ParamConstants.SCHEMA_TYPE_DOC)
				.define(ParamConstants.WRITE_MODE_PARAM, Type.STRING,
						ParamConstants.WRITE_MODE_UPSERT,
						ConfigDef.ValidString.in(
								ParamConstants.WRITE_MODE_UPSERT,
								ParamConstants.WRITE_MODE_INSERT),
						Importance.MEDIUM, ParamConstants.WRITE_MODE_DOC)
				.define(ParamConstants.TTL_PARAM, Type.INT,
						0,
						Importance.MEDIUM, ParamConstants.TTL_DOC)
				.define(ParamConstants.AUTO_CREATE_PARAM, Type.BOOLEAN,
						ParamConstants.AUTO_CREATE_DEFAULT,
						Importance.MEDIUM, ParamConstants.AUTO_CREATE_DOC)
				.define(ParamConstants.DRIVING_SIDE_PARAM, Type.STRING,
						ParamConstants.DRIVING_SIDE_KAFKA,
						Importance.HIGH, ParamConstants.DRIVING_SIDE_DOC)
				.define(ParamConstants.TABLES_PARAM, Type.LIST,
						"",
						Importance.HIGH, ParamConstants.TABLES_DOC)
				.define(ParamConstants.TABLENAME_MAPPER_PARAM, Type.STRING,
						ParamConstants.TABLENAME_MAPPER_DEFAULT,
						Importance.MEDIUM, ParamConstants.TABLENAME_MAPPER_DOC)
				.define(ParamConstants.KAFKA_KEY_FLD_GETTER_PARAM, Type.STRING,
						ParamConstants.KAFKA_KEY_FLD_GETTER_DEFAULT,
						Importance.MEDIUM, ParamConstants.KAFKA_KEY_FLD_GETTER_DOC)
				.define(ParamConstants.KAFKA_VALUE_FLD_GETTER_PARAM, Type.STRING,
						ParamConstants.KAFKA_VALUE_FLD_GETTER_DEFAULT,
						Importance.MEDIUM, ParamConstants.KAFKA_VALUE_FLD_GETTER_DOC)
				.define(ParamConstants.KAFKA_DELETE_EVENT_QUALIFIER_PARAM, Type.STRING,
						ParamConstants.KAFKA_DELETE_EVENT_QUALIFIER_DEFAULT,
						Importance.MEDIUM, ParamConstants.KAFKA_DELETE_EVENT_QUALIFIER_DOC)
				.define(ParamConstants.BACKOFF_MILLIS_PARAM, Type.INT,
						ParamConstants.BACKOFF_MILLIS_DEFAULT,
						Importance.LOW, ParamConstants.BACKOFF_MILLIS_DOC)
				.define(ParamConstants.METADATA_WAIT_PARAM, Type.LONG,
						ParamConstants.METADATA_WAIT_DEFAULT,
						Importance.LOW, ParamConstants.METADATA_WAIT_DOC)
				.define(ParamConstants.KEYSPACE_CREATE_ENABLED_PARAM, Type.BOOLEAN,
						false,
						Importance.LOW, ParamConstants.KEYSPACE_CREATE_ENABLED_DOC)
				.define(ParamConstants.KEYSPACE_CREATE_NUM_REPLICAS_PARAM, Type.INT,
						1,
						Importance.LOW, ParamConstants.KEYSPACE_CREATE_NUM_REPLICAS_DOC)
				.define(ParamConstants.OFFSET_FLUSH_BATCH_PARAM, Type.INT,
						ParamConstants.OFFSET_FLUSH_BATCH_DEFAULT,
						Importance.LOW, ParamConstants.OFFSET_FLUSH_BATCH_DOC)
				;
	}

	public CassandraSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSinkConnectorConfig.class);

	public int getSchemaType() {
		final String schemaType = this.getString(ParamConstants.SCHEMA_TYPE_PARAM);
		if (StringUtils.equals(ParamConstants.SCHEMA_TYPE_DEBEZIUM, schemaType)) {
			return ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
		} else if (StringUtils.equals(ParamConstants.SCHEMA_TYPE_VALUE_ONLY, schemaType)) {
			return ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY;
		} else {
			// ParamConstants.SCHEMA_TYPE_KEY_VALUE
			return ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE;
		}
	}

	public KafkaToCassandraTableNameMapper getTableNameMapper() {
		final String className = this.getString(ParamConstants.TABLENAME_MAPPER_PARAM); 
		LOGGER.info("Connector will will use {} for mapping between Kafka topic and Cassandra table",
				className);
		try {
			final Class<?> clazz = Class.forName(className);
			final Constructor<?> constructor = clazz.getConstructor();
			return (KafkaToCassandraTableNameMapper) constructor.newInstance();
		} catch (ClassNotFoundException nfe) {
			LOGGER.error("ClassNotFoundException while instantiating {}", className);
			throw new ConnectException("ClassNotFoundException while instantiating " + className, nfe);
		} catch (NoSuchMethodException nme) {
			LOGGER.error("NoSuchMethodException while instantiating {}", className);
			throw new ConnectException("NoSuchMethodException while instantiating " + className, nme);
		} catch (SecurityException se) {
			LOGGER.error("SecurityException while instantiating {}", className);
			throw new ConnectException("SecurityException while instantiating " + className, se);
		} catch (InvocationTargetException ite) {
			LOGGER.error("InvocationTargetException while instantiating {}", className);
			throw new ConnectException("InvocationTargetException while instantiating " + className, ite);
		} catch (IllegalAccessException iae) {
			LOGGER.error("IllegalAccessException while instantiating {}", className);
			throw new ConnectException("IllegalAccessException while instantiating " + className, iae);
		} catch (InstantiationException ie) {
			LOGGER.error("InstantiationException while instantiating {}", className);
			throw new ConnectException("InstantiationException while instantiating " + className, ie);
		}
	}

	public KafkaKeyFieldsGetter getKeyFieldsGetter() {
		final String className = this.getString(ParamConstants.KAFKA_KEY_FLD_GETTER_PARAM); 
		LOGGER.info("Connector will will use {} for getting list of key fields in Kafka topic",
				className);
		try {
			final Class<?> clazz = Class.forName(className);
			final Constructor<?> constructor = clazz.getConstructor();
			return (KafkaKeyFieldsGetter) constructor.newInstance();
		} catch (ClassNotFoundException nfe) {
			LOGGER.error("ClassNotFoundException while instantiating {}", className);
			throw new ConnectException("ClassNotFoundException while instantiating " + className, nfe);
		} catch (NoSuchMethodException nme) {
			LOGGER.error("NoSuchMethodException while instantiating {}", className);
			throw new ConnectException("NoSuchMethodException while instantiating " + className, nme);
		} catch (SecurityException se) {
			LOGGER.error("SecurityException while instantiating {}", className);
			throw new ConnectException("SecurityException while instantiating " + className, se);
		} catch (InvocationTargetException ite) {
			LOGGER.error("InvocationTargetException while instantiating {}", className);
			throw new ConnectException("InvocationTargetException while instantiating " + className, ite);
		} catch (IllegalAccessException iae) {
			LOGGER.error("IllegalAccessException while instantiating {}", className);
			throw new ConnectException("IllegalAccessException while instantiating " + className, iae);
		} catch (InstantiationException ie) {
			LOGGER.error("InstantiationException while instantiating {}", className);
			throw new ConnectException("InstantiationException while instantiating " + className, ie);
		}
	}

	public KafkaValueFieldsGetter getValueFieldsGetter() {
		final String className = this.getString(ParamConstants.KAFKA_VALUE_FLD_GETTER_PARAM); 
		LOGGER.info("Connector will will use {} for getting list of value fields in Kafka topic",
				className);
		try {
			final Class<?> clazz = Class.forName(className);
			final Constructor<?> constructor = clazz.getConstructor();
			return (KafkaValueFieldsGetter) constructor.newInstance();
		} catch (ClassNotFoundException nfe) {
			LOGGER.error("ClassNotFoundException while instantiating {}", className);
			throw new ConnectException("ClassNotFoundException while instantiating " + className, nfe);
		} catch (NoSuchMethodException nme) {
			LOGGER.error("NoSuchMethodException while instantiating {}", className);
			throw new ConnectException("NoSuchMethodException while instantiating " + className, nme);
		} catch (SecurityException se) {
			LOGGER.error("SecurityException while instantiating {}", className);
			throw new ConnectException("SecurityException while instantiating " + className, se);
		} catch (InvocationTargetException ite) {
			LOGGER.error("InvocationTargetException while instantiating {}", className);
			throw new ConnectException("InvocationTargetException while instantiating " + className, ite);
		} catch (IllegalAccessException iae) {
			LOGGER.error("IllegalAccessException while instantiating {}", className);
			throw new ConnectException("IllegalAccessException while instantiating " + className, iae);
		} catch (InstantiationException ie) {
			LOGGER.error("InstantiationException while instantiating {}", className);
			throw new ConnectException("InstantiationException while instantiating " + className, ie);
		}
	}

	public KafkaDeleteEventQualifier getDeleteEventQualifier() {
		final String className = this.getString(ParamConstants.KAFKA_DELETE_EVENT_QUALIFIER_PARAM); 
		LOGGER.info("Connector will will use {} for qualifying operation as deletion",
				className);
		try {
			final Class<?> clazz = Class.forName(className);
			final Constructor<?> constructor = clazz.getConstructor();
			return (KafkaDeleteEventQualifier) constructor.newInstance();
		} catch (ClassNotFoundException nfe) {
			LOGGER.error("ClassNotFoundException while instantiating {}", className);
			throw new ConnectException("ClassNotFoundException while instantiating " + className, nfe);
		} catch (NoSuchMethodException nme) {
			LOGGER.error("NoSuchMethodException while instantiating {}", className);
			throw new ConnectException("NoSuchMethodException while instantiating " + className, nme);
		} catch (SecurityException se) {
			LOGGER.error("SecurityException while instantiating {}", className);
			throw new ConnectException("SecurityException while instantiating " + className, se);
		} catch (InvocationTargetException ite) {
			LOGGER.error("InvocationTargetException while instantiating {}", className);
			throw new ConnectException("InvocationTargetException while instantiating " + className, ite);
		} catch (IllegalAccessException iae) {
			LOGGER.error("IllegalAccessException while instantiating {}", className);
			throw new ConnectException("IllegalAccessException while instantiating " + className, iae);
		} catch (InstantiationException ie) {
			LOGGER.error("InstantiationException while instantiating {}", className);
			throw new ConnectException("InstantiationException while instantiating " + className, ie);
		}
	}

}
