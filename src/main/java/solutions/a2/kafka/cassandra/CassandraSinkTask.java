package solutions.a2.kafka.cassandra;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.Version;


/**
 * Cassandra/ScyllaDB/Amazon Keyspaces Sink Connector Task
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraSinkTask extends SinkTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSinkTask.class);

	private CassandraWrapper cw;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
	private int offsetFlushSize;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		final String connectorName = props.get("name");
		LOGGER.info("Starting Cassandra Sink Task for connector '{}'", connectorName);
		CassandraSinkConnectorConfig config = new CassandraSinkConnectorConfig(props);
		offsetFlushSize = config.getInt(ParamConstants.OFFSET_FLUSH_BATCH_PARAM);
		cw = new CassandraWrapper(connectorName, config);
		currentOffsets = new HashMap<>();
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		currentOffsets.clear();
		int processedRecords = 0;
		for (SinkRecord record : records) {
			cw.processSinkRecord(record);
			currentOffsets.put(
					new TopicPartition(record.topic(), record.kafkaPartition()),
					new OffsetAndMetadata(record.kafkaOffset()));
			processedRecords++;
			if (processedRecords == offsetFlushSize) {
				this.flush(currentOffsets);
				currentOffsets.clear();
				processedRecords = 0;
			}
		}
		if (processedRecords > 0) {
			this.flush(currentOffsets);
			currentOffsets.clear();
		}
	}

	@Override
	public void stop() {
		if (cw != null) {
			cw.destroy();
		}
	}

}
