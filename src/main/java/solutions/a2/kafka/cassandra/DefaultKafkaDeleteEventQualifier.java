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

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * DefaultKafkaDeleteEventQualifier - does this Sink Record is delete event?
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class DefaultKafkaDeleteEventQualifier implements KafkaDeleteEventQualifier {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaDeleteEventQualifier.class);

	@Override
	public boolean isDeleteEvent(SinkRecord record, int schemaType) {
		//Do we pass delete as 'op'="d" or 'op'="D"?
		Iterator<Header> iterator = record.headers().iterator();
		while (iterator.hasNext()) {
			Header header = iterator.next();
			if (StringUtils.equals(header.key(), "op")) {
				final Object value = header.value();
				if (value instanceof String) {
					if (StringUtils.equalsIgnoreCase("D", (String) value)) {
						LOGGER.debug("'delete' operation detected in topic/partition {}/{} at offset {} using header 'op'=\"{}\".",
								record.topic(), record.kafkaPartition(), record.kafkaOffset(), value);
						return true;
					}
				}
			}
		}
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE) {
			if (record.value() == null) {
				LOGGER.debug("'delete' operation detected in topic/partition {}/{} at offset {} due to missed value part.",
						record.topic(), record.kafkaPartition(), record.kafkaOffset());
				return true;
			}
		} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			if (record.value() != null) {
				if (StringUtils.equalsIgnoreCase("D", ((Struct) record.value()).getString("op"))) {
					LOGGER.debug("'delete' operation detected in topic/partition {}/{} at offset {} using Debezium payload with op='d'.",
							record.topic(), record.kafkaPartition(), record.kafkaOffset());
					return true;
				}
			}
		} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY)
			// In this case we unable to detect delete operation (((
			LOGGER.debug("Unable to detect 'delete' operation in topic/partition {}/{} at offset {} for '{}' schema type.",
					record.topic(), record.kafkaPartition(), record.kafkaOffset(), ParamConstants.SCHEMA_TYPE_VALUE_ONLY);
			return false;
		}
		LOGGER.debug("'delete' operation not detected in topic/partition {}/{} at offset {}.",
				record.topic(), record.kafkaPartition(), record.kafkaOffset());
		return false;
	}

}
