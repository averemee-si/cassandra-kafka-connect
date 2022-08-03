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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

import solutions.a2.kafka.cassandra.jmx.CassandraSinkMetrics;

/**
 * Casandra table object
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTable.class);

	private final PreparedStatement insert;
	private final PreparedStatement delete;
	private final List<CassandraColumn> pkColumns;
	private final List<CassandraColumn> allColumns;
	private final int schemaType;
	private final CqlSession session;
	private final CassandraSinkMetrics metrics;

	
	public CassandraTable(
			final CassandraWrapper cw, final String tableName, final SinkRecord record) {

		final DriverExecutionProfile profile = cw.cqlSession().getContext().getConfig().getDefaultProfile();
		final boolean useV5Protocol = ! StringUtils.equalsIgnoreCase(
				profile.getString(DefaultDriverOption.PROTOCOL_VERSION, CassandraWrapper.DEFAULT_PROTOCOL_VERSION),
				CassandraWrapper.DEFAULT_PROTOCOL_VERSION);

		this.pkColumns = new ArrayList<>();
		this.allColumns = new ArrayList<>();
		this.schemaType = cw.schema();
		this.session = cw.cqlSession();
		this.metrics = new CassandraSinkMetrics(cw.connector(), cw.keyspace(), tableName); 

		TableMetadata tableMetadata = session
				.getMetadata()
				.getKeyspace(cw.keyspace()).get()
				.getTable(tableName).get();
		Object insertInto = QueryBuilder.insertInto(tableName);
		Object deleteFrom = QueryBuilder.deleteFrom(tableName);

		final List<Field> keyFields;
		final List<Field> valueFields;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE) {
			keyFields = record.keySchema().fields();
			valueFields = record.valueSchema().fields();
		} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY) {
			keyFields = null;
			valueFields = record.valueSchema().fields();			
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			keyFields = null;
			valueFields = record.valueSchema().field("after").schema().fields();
		}

		boolean firstInsertInto = true;
		boolean firstDeleteFrom = true;
		for (ColumnMetadata columnMetadata : tableMetadata.getColumns().values()) {
			boolean isPartOfPk = tableMetadata.getPrimaryKey().contains(columnMetadata);
			CassandraColumn cColumn = null;
			boolean supportedType = true;
			try {
				cColumn = new CassandraColumn(columnMetadata, keyFields, valueFields);
			} catch (UnsupportedColumnDataTypeException uce) {
				if (isPartOfPk) {
					LOGGER.error("Unable to map primary key column '{}' in table '{}.{}'. Unsupported data type!",
							uce.getColumnName(), cw.keyspace(), tableName);
					throw new ConnectException("Unable to perform Cassandra primary key mapping!");
				} else {
					supportedType = false;
					LOGGER.warn("Column '{}' in table '{}.{}' is ignored due to unsupported data type",
							uce.getColumnName(), cw.keyspace(), tableName);
				}
			}
			if (supportedType) {
				final String columnName = cColumn.getColumnName();				
				if (isPartOfPk && cColumn.getNameInTopic() == null) {
					LOGGER.error("Unable to map primary key column '{}' in table '{}.{}'. Not present in Kafka topic!",
							columnName, cw.keyspace(), tableName);
					throw new ConnectException("Unable to perform Cassandra primary key mapping!");
				} else if (cColumn.getNameInTopic() == null) {
					LOGGER.warn("Column '{}' in table '{}.{}' is not present in Kafka topic!",
							columnName, cw.keyspace(), tableName);
				} else {
					if (firstInsertInto) {
						insertInto = ((InsertInto)insertInto).value(columnName, QueryBuilder.bindMarker());
						firstInsertInto = false;
					} else {
						insertInto = ((RegularInsert)insertInto).value(columnName, QueryBuilder.bindMarker());
					}
					if (isPartOfPk) {
						if (firstDeleteFrom) {
							deleteFrom = ((DeleteSelection)deleteFrom)
									.whereColumn(columnName)
									.isEqualTo(QueryBuilder.bindMarker());
							firstDeleteFrom = false;
						} else {
							deleteFrom = ((Delete)deleteFrom)
									.whereColumn(columnName)
									.isEqualTo(QueryBuilder.bindMarker());
						}
						pkColumns.add(cColumn);
					}
					allColumns.add(cColumn);
				}
			}
		}



		if (cw.ttl() != null) {
			((RegularInsert)insertInto).usingTtl(cw.ttl());
		}
		if (useV5Protocol) {
			delete = session.prepare(((Delete)deleteFrom)
					.build()
					.setKeyspace(cw.keyspace())
					.setConsistencyLevel(cw.consistency())
					.setTimeout(cw.timeout()));
		} else {
			delete = session.prepare(((Delete)deleteFrom)
					.build()
					.setConsistencyLevel(cw.consistency())
					.setTimeout(cw.timeout()));
		}
		if (cw.upsert()) {
			if (useV5Protocol) {
				insert = session.prepare(((RegularInsert)insertInto)
						.build()
						.setKeyspace(cw.keyspace())
						.setConsistencyLevel(cw.consistency())
						.setTimeout(cw.timeout()));
			} else {
				insert = session.prepare(((RegularInsert)insertInto)
						.build()
						.setConsistencyLevel(cw.consistency())
						.setTimeout(cw.timeout()));
			}
		} else {
			if (useV5Protocol) {
				insert = session.prepare(((RegularInsert)insertInto)
						.ifNotExists()
						.build()
						.setKeyspace(cw.keyspace())
						.setConsistencyLevel(cw.consistency())
						.setTimeout(cw.timeout()));
			} else {
				insert = session.prepare(((RegularInsert)insertInto)
						.ifNotExists()
						.build()
						.setConsistencyLevel(cw.consistency())
						.setTimeout(cw.timeout()));
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Upsert/Insert for table '{}.{}' will be executed using\n{}",
					cw.keyspace(), tableName, insert.getQuery());
			LOGGER.debug("Delete for table '{}.{}' will be executed using\n{}",
					cw.keyspace(), tableName, delete.getQuery());
		}
	}

	public void processSinkRecord(final boolean deleteOp, final SinkRecord record) {
		BoundStatement bs;
		final Struct keyStruct;
		final Struct valueStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE) {
			keyStruct = (Struct) record.key();
			valueStruct = (Struct) record.value();
		} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY) {
			keyStruct = null;
			valueStruct = (Struct) record.value();			
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			keyStruct = null;
			if (deleteOp) {
				valueStruct = ((Struct) record.value()).getStruct("before");
			} else {
				valueStruct = ((Struct) record.value()).getStruct("after");
			}
		}

		long bindNanos = System.nanoTime();
		if (deleteOp) {
			bs = delete.bind();
			int columnNo = 0;
			for (CassandraColumn column : pkColumns) {
				bs = column.bind(bs, columnNo, keyStruct, valueStruct);
				columnNo++;
			}
		} else {
			bs = insert.bind();
			int columnNo = 0;
			for (CassandraColumn column : allColumns) {
				bs = column.bind(bs, columnNo, keyStruct, valueStruct);
				columnNo++;
			}
		}
		bindNanos = System.nanoTime() - bindNanos;

		long execNanos = System.nanoTime();
		session.execute(bs);
		execNanos = System.nanoTime() - execNanos;

		metrics.addMetrics(!deleteOp, bindNanos, execNanos);

	}

}
