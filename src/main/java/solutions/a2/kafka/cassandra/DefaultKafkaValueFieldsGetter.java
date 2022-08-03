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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * DefaultKafkaValueFieldsGetter - extracts only value fields defs from Kafka Sink Record
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class DefaultKafkaValueFieldsGetter implements KafkaValueFieldsGetter {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaValueFieldsGetter.class);

	@Override
	public List<Field> getValueFields(final SinkRecord record, final int schemaType, final List<Field> keyFields) {
		final List<Field> valueFields;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE) {
			if (record.valueSchema() != null) {
				valueFields = record.valueSchema().fields();
			} else {
				LOGGER.warn("Unable to extract value fields from topic '{}'", record.topic());
				valueFields = new ArrayList<>();
			}
		} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			if (record.valueSchema() != null) {
				valueFields = record.valueSchema().field("after").schema().fields();
			} else {
				LOGGER.warn("Unable to extract value fields from topic '{}'", record.topic());
				valueFields = new ArrayList<>();
			}
		} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY)
			// In this case we assume, that value fields are all optional fields...
			valueFields = new ArrayList<>();
			if (record.valueSchema() != null && record.valueSchema().fields().size() > 0) {
				record.valueSchema().fields().forEach(field -> {
					if (field.schema().isOptional()) {
						valueFields.add(field);
					}
				});
			} else {
				LOGGER.error("Unable to get schema from topic '{}'!", record.topic());
				throw new ConnectException("Unable to get schema from topic " + record.topic() + "!");
			}
		}
		return valueFields;
	}

}
