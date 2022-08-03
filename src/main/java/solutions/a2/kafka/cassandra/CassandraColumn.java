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

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;
import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.BLOB;
import static com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN;
import static com.datastax.oss.driver.api.core.type.DataTypes.DATE;
import static com.datastax.oss.driver.api.core.type.DataTypes.DECIMAL;
import static com.datastax.oss.driver.api.core.type.DataTypes.DOUBLE;
import static com.datastax.oss.driver.api.core.type.DataTypes.DURATION;
import static com.datastax.oss.driver.api.core.type.DataTypes.FLOAT;
import static com.datastax.oss.driver.api.core.type.DataTypes.INET;
import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.SMALLINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIME;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMESTAMP;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.oss.driver.api.core.type.DataTypes.TINYINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.UUID;
import static com.datastax.oss.driver.api.core.type.DataTypes.VARINT;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Casandra2Kafka/Table2Topic and vice versa mapping
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraColumn {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraColumn.class);

	private String columnName;
	private String dataType;
	private boolean isKeyStruct;
	private String nameInTopic;
	private Schema schemaInTopic;


	/**
	 * Construct CassandraColumn and maps it to Kafka field in topic
	 * belonging to partition and primary keys
	 * 
	 * @param metadata Cassandra column metadata
	 * @param keyFields
	 * @param valueFields
	 */
	public CassandraColumn(
			final ColumnMetadata metadata, final List<Field> keyFields, final List<Field> valueFields) 
					 throws UnsupportedColumnDataTypeException {
		columnName = metadata.getName().asCql(true);
		dataType = StringUtils.lowerCase(metadata.getType().toString());
		if (metadata.getType().equals(ASCII) ||
				metadata.getType().equals(BIGINT) ||
				metadata.getType().equals(BLOB) ||
				metadata.getType().equals(BOOLEAN) ||
				metadata.getType().equals(DECIMAL) ||
				metadata.getType().equals(DOUBLE) ||
				metadata.getType().equals(FLOAT) ||
				metadata.getType().equals(INT) ||
				metadata.getType().equals(TIMESTAMP) ||
				metadata.getType().equals(UUID) ||
				metadata.getType().equals(VARINT) ||
				metadata.getType().equals(TIMEUUID) ||
				metadata.getType().equals(INET) ||
				metadata.getType().equals(DATE) ||
				metadata.getType().equals(TEXT) ||
				metadata.getType().equals(TIME) ||
				metadata.getType().equals(SMALLINT) ||
				metadata.getType().equals(TINYINT) ||
				metadata.getType().equals(DURATION)) {
			boolean mapped = false;
			if (keyFields != null && keyFields.size() > 0) {
				mapped = mapColunmToField(keyFields, true);
			}
			if (!mapped && valueFields != null & valueFields.size() > 0) {
				mapped = mapColunmToField(valueFields, false);
			}
		} else {
			throw new UnsupportedColumnDataTypeException(columnName);
		}
	}

	private boolean mapColunmToField(final List<Field> fields, final boolean isKey) {
		boolean mapped = false;
		final String unescapedName;
		if ('\"' == columnName.charAt(0) && '\"' == columnName.charAt(columnName.length() - 1)) {
			unescapedName = StringUtils.substring(columnName, 1, columnName.length() - 1);
		} else {
			unescapedName = columnName;
		}
		for (Field field : fields) {
			if (StringUtils.equalsIgnoreCase(unescapedName, field.name())) {
				isKeyStruct = isKey;
				nameInTopic = field.name();
				//TODO
				//TODO - need to check between Kafka and Cassandra data types!!!
				//TODO
				schemaInTopic = field.schema();
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Column '{}' with type '{}' is mapped to Kafka {} field '{}' with type '{}'",
							columnName, dataType, (isKeyStruct ? "keyStruct" : "valueStruct"), nameInTopic, 
							(schemaInTopic.schema().name() == null ? schemaInTopic.schema().toString() : schemaInTopic.schema().name()));
				}
				mapped = true;
				break;
			}
		}
		return mapped;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getNameInTopic() {
		return nameInTopic;
	}

	public BoundStatement bind(final BoundStatement bs, final int position, final Struct keyStruct, final Struct valueStruct) {
		final Object valueFromTopic = isKeyStruct ? keyStruct.get(nameInTopic) : valueStruct.get(nameInTopic);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("\tValue of column '{}', position={},  isKeyStruct={} with datatype '{}' is set to '{}' from topic field '{}'",
					columnName, position, isKeyStruct, dataType, valueFromTopic, nameInTopic);
		}
		try {
			if (valueFromTopic == null) {
				return bs.setToNull(position);
			} else {
				switch (dataType) {
				case "boolean":
					return bs.setBoolean(position,  (boolean) valueFromTopic);
				case "tinyint":
					if (valueFromTopic instanceof Byte) {
						return bs.setByte(position, (byte) valueFromTopic);
					} else if (valueFromTopic instanceof Double) {
						return bs.setByte(position, ((Double) valueFromTopic).byteValue());
					} else {
						final String  errMsg = String.format(
								"Unable to cast object of type %s to Cassandra byte!",
								valueFromTopic.getClass().getCanonicalName());
						LOGGER.error(errMsg);
						throw new ConnectException(errMsg);
					}
				case "smallint":
					if (valueFromTopic instanceof Short) {
						return bs.setShort(position, (short) valueFromTopic);
					} else if (valueFromTopic instanceof Double) {
						return bs.setShort(position, ((Double) valueFromTopic).shortValue());
					} else {
						final String  errMsg = String.format(
								"Unable to cast object of type %s to Cassandra short!",
								valueFromTopic.getClass().getCanonicalName());
						LOGGER.error(errMsg);
						throw new ConnectException(errMsg);
					}
				case "int":
					if (valueFromTopic instanceof Integer) {
						return bs.setInt(position, (int) valueFromTopic);
					} else if (valueFromTopic instanceof Double) {
						return bs.setInt(position, ((Double) valueFromTopic).intValue());
					} else {
						final String  errMsg = String.format(
								"Unable to cast object of type %s to Cassandra int!",
								valueFromTopic.getClass().getCanonicalName());
						LOGGER.error(errMsg);
						throw new ConnectException(errMsg);
					}
				case "bigint":
				case "varint":
					if (valueFromTopic instanceof Long) {
						return bs.setLong(position, (long) valueFromTopic);
					} else if (valueFromTopic instanceof Integer) {
						return bs.setLong(position, (int) valueFromTopic);
					} else if (valueFromTopic instanceof Double) {
						return bs.setLong(position, ((Double) valueFromTopic).longValue());
					} else {
						final String  errMsg = String.format(
								"Unable to cast object of type %s to Cassandra long!",
								valueFromTopic.getClass().getCanonicalName());
						LOGGER.error(errMsg);
						throw new ConnectException(errMsg);
					}
				case "float":
					return bs.setFloat(position, (float) valueFromTopic);
				case "double":
					return bs.setDouble(position, (double) valueFromTopic);
				case "decimal":
					return bs.setBigDecimal(position, (BigDecimal) valueFromTopic);
				case "ascii":
				case "text":
					return bs.setString(position, (String) valueFromTopic);
				case "blob":
					return bs.setByteBuffer(position, (ByteBuffer) valueFromTopic);
				case "uuid":
				case "timeuuid":
					try {
						return bs.setUuid(position, java.util.UUID.fromString((String) valueFromTopic));
					} catch (IllegalArgumentException iae) {
						LOGGER.error("Unable to convert '{}' to UUID!", (String) valueFromTopic);
						throw new ConnectException("Wrong value for UUID!");
					}
				case "inet":
					try {
						return bs.setInetAddress(columnName, InetAddress.getByName((String) valueFromTopic));
					} catch (UnknownHostException e) {
						LOGGER.error("Unable to get InetAddress for '{}'!", (String) valueFromTopic);
						throw new ConnectException("Wrong value for InetAddress!");
					}
				case "duration":
					try {
						return bs.setCqlDuration(position, CqlDuration.from((String) valueFromTopic));
					} catch (IllegalArgumentException iae) {
						LOGGER.error("Unable to convert '{}' to CqlDuration!", (String) valueFromTopic);
						return bs.setToNull(position);
					}
				case "timestamp":
					//TODO
					//TODO - TZ!
					//TODO
					return bs.setInstant(position, Instant.ofEpochMilli(((java.util.Date) valueFromTopic).getTime()));
				case "date":
					//TODO
					//TODO - TZ!
					//TODO
					return bs.setLocalDate(position,
							LocalDate.ofInstant(
									Instant.ofEpochMilli(((java.util.Date) valueFromTopic).getTime()),
									ZoneId.systemDefault()));
				case "time":
					//TODO
					//TODO - TZ!
					//TODO
					return bs.setLocalTime(position,
							LocalTime.ofInstant(
									Instant.ofEpochMilli(((java.util.Date) valueFromTopic).getTime()),
									ZoneId.systemDefault()));
				}
			}
			return bs;
		} catch (ClassCastException cce) {
			LOGGER.error(cce.getMessage());
			LOGGER.error("\tUnable to set value of column '{}', position={},  isKeyStruct={} with datatype '{}' is set to '{}' from topic field '{}'",
					columnName, position, isKeyStruct, dataType, valueFromTopic, nameInTopic);
			throw new ConnectException(cce);
		}
	}

	public static DataType dataTypeFromKafkaSchema(Schema schema) {
		final String typeFromSchema = StringUtils.upperCase(schema.type().getName());
		switch (typeFromSchema) {
		case "BOOLEAN":
			return BOOLEAN;
		case "INT8":
			return TINYINT;
		case "INT16":
			return SMALLINT;
		case "INT32":
			if (schema.name() != null && StringUtils.equals(schema.name(), Date.LOGICAL_NAME)) {
				return DATE;
			} else {
				return INT;
			}
		case "INT64":
			if (schema.name() != null && StringUtils.equals(schema.name(), Timestamp.LOGICAL_NAME)) {
				return TIMESTAMP;
			} else {
				return BIGINT;
			}
		case "FLOAT32":
			return FLOAT;
		case "FLOAT64":
			return DOUBLE;
		case "BYTES":
			if (schema.name() != null && StringUtils.equals(schema.name(), Decimal.LOGICAL_NAME)) {
				return DECIMAL;
			} else {
				return BLOB;
			}
		case "STRING":
			return TEXT;
		default:
			//TODO
			//TODO throw an Exception
			//TODO
			return null;
		}
	}

}
