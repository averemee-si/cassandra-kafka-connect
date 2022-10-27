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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Integration tests for connector
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraObjectsIT {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraObjectsIT.class);

	private static Map<String, String> props = new HashMap<>();
	private static CassandraSinkConnectorConfig config = null;
	private static CassandraWrapper cw;
	private static Schema simpleSchema;
	private static Schema simpleKeySchema;
	private static Schema simpleValueSchema;
	private static Schema debeziumKeySchema;
	private static Schema debeziumValueSchema;
	private static Schema debeziumEnvelopeSchema;
	private static List<SinkRecord> simpleInsertList;
	private static List<SinkRecord> simpleDeleteList;
	private static List<SinkRecord> simpleKeyValueInsertList;
	private static List<SinkRecord> simpleKeyValueDeleteList;
	private static List<SinkRecord> debeziumInsertList;
	private static List<SinkRecord> debeziumDeleteList;

	private static String simpleTopic = "simple_topic";
	private static String simpleKeyValueTopic = "simple_key_value_topic";
	private static String debeziumTopic = "debezium_topic";

	@BeforeAll
	public static void beforeClass() {
		BasicConfigurator.configure();

		LOGGER.info("Starting tests...");

		props.put(ParamConstants.CONTACT_POINTS_PARAM, "localhost");
		props.put(ParamConstants.LOCAL_DATACENTER_PARAM, "datacenter1");

		// AWS # 01
//		props.put(ParamConstants.CONTACT_POINTS_PARAM, "cassandra.eu-north-1.amazonaws.com");
//		props.put(ParamConstants.LOCAL_DATACENTER_PARAM, "eu-north-1");
//		props.put(ParamConstants.SECURITY_PARAM, ParamConstants.SECURITY_AWS_SIGV4);

		// Astra DB
//		props.put(ParamConstants.ASTRA_DB_CONNECT_BUNDLE_PARAM, "<PATH-TO>/secure-connect-astradb.zip");
//		props.put(ParamConstants.ASTRA_DB_CLIENT_ID_PARAM, "<CLIENT-ID>");
//		props.put(ParamConstants.ASTRA_DB_CLIENT_SECRET_PARAM, "<CLIENT-SECRET>");
//		props.put(ParamConstants.SECURITY_PARAM, ParamConstants.SECURITY_ASTRA_DB);

		props.put(ParamConstants.KEYSPACE_CREATE_ENABLED_PARAM, "true");
	
		props.put(ParamConstants.KEYSPACE_PARAM, "test");

		// Yes, we will create table from topic definition
		props.put(ParamConstants.AUTO_CREATE_PARAM, "true");
		try {
			config = new CassandraSinkConnectorConfig(props);
		} catch (ConfigException ce) {
			ce.printStackTrace();
			System.exit(1);
		}

	}

	@AfterAll
	public static void afterClass() {
		cw.destroy();
	}

	@Test
	void testSinkConnector() {
		setupSchemas();
		setupTestData();

		// KEY_VALUE message (oracdc default)
		props.put(ParamConstants.SCHEMA_TYPE_PARAM, ParamConstants.SCHEMA_TYPE_KEY_VALUE);
		try {
			config = new CassandraSinkConnectorConfig(props);
		} catch (ConfigException ce) {
			ce.printStackTrace();
			System.exit(1);
		}
		cw = new CassandraWrapper("connector-test01", config);
		put(simpleKeyValueInsertList);
		cw.destroy();

		// VALUE only message (Confluent JDBC default)
		props.put(ParamConstants.SCHEMA_TYPE_PARAM, ParamConstants.SCHEMA_TYPE_VALUE_ONLY);
		try {
			config = new CassandraSinkConnectorConfig(props);
		} catch (ConfigException ce) {
			ce.printStackTrace();
			System.exit(1);
		}
		cw = new CassandraWrapper("connector-test02", config);
		put(simpleInsertList);
		cw.destroy();

		// Debezium format message
		props.put(ParamConstants.SCHEMA_TYPE_PARAM, ParamConstants.SCHEMA_TYPE_DEBEZIUM);
		try {
			config = new CassandraSinkConnectorConfig(props);
		} catch (ConfigException ce) {
			ce.printStackTrace();
			System.exit(1);
		}
		cw = new CassandraWrapper("connector-test03", config);
		put(debeziumInsertList);
		cw.destroy();
	}

	private static void setupSchemas() {

		final SchemaBuilder simpleSchemaBuilder = SchemaBuilder
				.struct()
				.name("SimpleSchema")
				.doc("Simple schema - keys and values together in sibgle structure");
		simpleSchemaBuilder.field("BIGINT_ID", Schema.INT64_SCHEMA);
		simpleSchemaBuilder.field("INT_VALUE", Schema.OPTIONAL_INT32_SCHEMA);
		simpleSchemaBuilder.field("Description", Schema.OPTIONAL_STRING_SCHEMA);
		simpleSchemaBuilder.field("Date_From", Date.SCHEMA);
		simpleSchemaBuilder.field("Timestamp_To", Timestamp.SCHEMA);
		simpleSchemaBuilder.field("Decimal_Data", Decimal.schema(2));
		simpleSchema = simpleSchemaBuilder.build(); 

		final SchemaBuilder simpleKeySchemaBuilder = SchemaBuilder
				.struct()
				.name("SimpleSchemaKeys")
				.doc("Simple schema for key's");
		simpleKeySchemaBuilder.field("BIGINT_ID", Schema.INT64_SCHEMA);
		simpleKeySchema = simpleKeySchemaBuilder.build();
		final SchemaBuilder simpleValueSchemaBuilder = SchemaBuilder
				.struct()
				.name("SimpleSchemaValues")
				.doc("Simple schema for value's");
		simpleValueSchemaBuilder.field("INT_VALUE", Schema.OPTIONAL_INT32_SCHEMA);
		simpleValueSchemaBuilder.field("Description", Schema.OPTIONAL_STRING_SCHEMA);
		simpleValueSchemaBuilder.field("Date_From", Date.SCHEMA);
		simpleValueSchemaBuilder.field("Timestamp_To", Timestamp.SCHEMA);
		simpleValueSchemaBuilder.field("Decimal_Data", Decimal.schema(2));
		simpleValueSchema = simpleValueSchemaBuilder.build();

		final SchemaBuilder debeziumKeySchemaBuilder = SchemaBuilder
				.struct()
				.name("DebeziumSchemaKeys")
				.doc("Debezium schema for key's");
		debeziumKeySchemaBuilder.field("BIGINT_ID", Schema.INT64_SCHEMA);
		debeziumKeySchema = debeziumKeySchemaBuilder.build();
		final SchemaBuilder debeziumBeforeAfterValueSchemaBuilder = SchemaBuilder
				.struct()
				.name("DebeziumSchemaValues")
				.optional()
				.doc("Debezium schema for value's");
		debeziumBeforeAfterValueSchemaBuilder.field("BIGINT_ID", Schema.INT64_SCHEMA);
		debeziumBeforeAfterValueSchemaBuilder.field("INT_VALUE", Schema.OPTIONAL_INT32_SCHEMA);
		debeziumBeforeAfterValueSchemaBuilder.field("Description", Schema.OPTIONAL_STRING_SCHEMA);
		debeziumBeforeAfterValueSchemaBuilder.field("Date_From", Date.SCHEMA);
		debeziumBeforeAfterValueSchemaBuilder.field("Timestamp_To", Timestamp.SCHEMA);
		debeziumBeforeAfterValueSchemaBuilder.field("Decimal_Data", Decimal.schema(2));
		final Schema debeziumBefore = debeziumBeforeAfterValueSchemaBuilder.build();
		final Schema debeziumAfter = debeziumBeforeAfterValueSchemaBuilder.build();
		debeziumValueSchema = debeziumBeforeAfterValueSchemaBuilder.build();
		final SchemaBuilder debeziumEnvelopeSchemaBuilder = SchemaBuilder
				.struct()
				.name("DebeziumEnvelope")
				.optional();
		debeziumEnvelopeSchemaBuilder.field("before", debeziumBefore);
		debeziumEnvelopeSchemaBuilder.field("after", debeziumAfter);
		debeziumEnvelopeSchemaBuilder.field("op", Schema.STRING_SCHEMA);
		debeziumEnvelopeSchemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
		debeziumEnvelopeSchema = debeziumEnvelopeSchemaBuilder.build();

	}

	private static void setupTestData() {
		simpleInsertList = new ArrayList<>();
		simpleDeleteList =  new ArrayList<>();
		simpleKeyValueInsertList = new ArrayList<>();
		simpleKeyValueDeleteList =  new ArrayList<>();
		debeziumInsertList = new ArrayList<>();
		debeziumDeleteList = new ArrayList<>();

		setupTestInsertData(simpleInsertList, simpleTopic, ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY);
		setupTestDeleteData(simpleDeleteList, simpleTopic, ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY);
		setupTestInsertData(simpleKeyValueInsertList, simpleKeyValueTopic, ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE);
		setupTestDeleteData(simpleKeyValueDeleteList, simpleKeyValueTopic, ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE);
		setupTestInsertData(debeziumInsertList, debeziumTopic, ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM);
		setupTestDeleteData(debeziumDeleteList, debeziumTopic, ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM);
	}

	private static void setupTestInsertData(
			List<SinkRecord> insertList, String topic, int testSchemaType) {
		switch (testSchemaType) {
		case ParamConstants.SCHEMA_TYPE_INT_KEY_VALUE:
			for (int id = 100; id < 110; id++) {
				Struct keyStruct = new Struct(simpleKeySchema);
				Struct valueStruct = new Struct(simpleValueSchema);
				keyStruct.put("BIGINT_ID", (long) id);
				valueStruct.put("INT_VALUE", id);
				valueStruct.put("Description", "Description for id=" + id);
				valueStruct.put("Date_From",
						java.sql.Date.from(Instant.now().minusSeconds(id)));
				valueStruct.put("Timestamp_To",
						java.sql.Timestamp.from(Instant.now().plus(id, ChronoUnit.DAYS)));
				valueStruct.put("Decimal_Data",
						(new BigDecimal(Math.random()).setScale(2, RoundingMode.HALF_UP)));
				SinkRecord record = new SinkRecord(topic, 0, simpleKeySchema, keyStruct, simpleValueSchema, valueStruct, id);
				insertList.add(record);
			}
			break;
		case ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM:
			for (int id = 200; id < 210; id++) {
				Struct keyStruct = new Struct(debeziumKeySchema);
				Struct valueStruct = new Struct(debeziumValueSchema);
				Struct envelopeStruct = new Struct(debeziumEnvelopeSchema);
				keyStruct.put("BIGINT_ID", (long) id);
				valueStruct.put("BIGINT_ID", (long) id);
				valueStruct.put("INT_VALUE", id);
				valueStruct.put("Description", "Description for id=" + id);
				valueStruct.put("Date_From",
						java.sql.Date.from(Instant.now().minusSeconds(id)));
				valueStruct.put("Timestamp_To",
						java.sql.Timestamp.from(Instant.now().plus(id, ChronoUnit.DAYS)));
				valueStruct.put("Decimal_Data",
						(new BigDecimal(Math.random()).setScale(2, RoundingMode.HALF_UP)));

				envelopeStruct.put("after", valueStruct);
				envelopeStruct.put("op", "c");
				envelopeStruct.put("ts_ms", System.currentTimeMillis());

				SinkRecord record = new SinkRecord(topic, 0, debeziumKeySchema, keyStruct, debeziumEnvelopeSchema, envelopeStruct, id);
				insertList.add(record);
			}
			break;
		case ParamConstants.SCHEMA_TYPE_INT_VALUE_ONLY:
			for (int id = 300; id < 310; id++) {
				Struct valueStruct = new Struct(simpleSchema);
				valueStruct.put("BIGINT_ID", (long) id);
				valueStruct.put("INT_VALUE", id);
				valueStruct.put("Description", "Description for id=" + id);
				valueStruct.put("Date_From",
						java.sql.Date.from(Instant.now().minusSeconds(id)));
				valueStruct.put("Timestamp_To",
						java.sql.Timestamp.from(Instant.now().plus(id, ChronoUnit.DAYS)));
				valueStruct.put("Decimal_Data",
						(new BigDecimal(Math.random()).setScale(2, RoundingMode.HALF_UP)));
				SinkRecord record = new SinkRecord(topic, 0, null, null, simpleSchema, valueStruct, id);
				insertList.add(record);
			}
			break;
		}
	}

	private static void setupTestDeleteData(
			List<SinkRecord> deleteList, String topic, int testSchemaType) {
		// TODO Auto-generated method stub
		
	}

	private static void put(Collection<SinkRecord> records) {
		for (SinkRecord record : records) {
			cw.processSinkRecord(record);
		}
	}

}
