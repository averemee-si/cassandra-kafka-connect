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

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * DefaultKafkaToCassandraTableNameMapper maps name from Kafka topic to 
 * Cassandra table name 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class DefaultKafkaToCassandraTableNameMapper implements KafkaToCassandraTableNameMapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaToCassandraTableNameMapper.class);

	@Override
	public String getTableName(final SinkRecord record, final int schemaType) {
		String tableName = null;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE ||
				schemaType == ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY ||
				schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			tableName = record.topic();
			LOGGER.debug("Table name from Kafka topic = '{}'.", tableName);
			// For message from Debezium table name can be extracted from message too
			// 			tableName = ((Struct) record.value()).getStruct("source").getString("table");

		}
		return tableName;
	}

}
