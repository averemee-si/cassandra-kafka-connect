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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.Version;

/**
 * Cassandra/ScyllaDB/Amazon Keyspaces Sink Connector
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraSinkConnector extends SinkConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSinkConnector.class);
	// Generated using 	https://patorjk.com/software/taag/#p=display&f=Big%20Chief&t=A2%20Solutions%0A%20%20%20%20Cassandra%0A%20%20%20%20%20%20%20%20%20%20%20%20Sink
	private static final String LOGO = 
			"__________________________________________________________________________\n" +
			"    __      __           __                                               \n" +
			"    / |   /    )       /    )         /               ,                   \n" +
			"---/__|----___/--------\\--------__---/---------_/_--------__----__---__- \n" +
			"  /   |  /              \\     /   ) /   /   /  /    /   /   ) /   ) (_ ` \n" +
			"_/____|_/____/______(____/___(___/_/___(___(__(_ __/___(___/_/___/_(__)_  \n" +
			"                                                                          \n" +
			"                                                                          \n" +
			"__________________________________________________________________________\n" +
			"                     __                                                   \n" +
			"                   /    )                                   /             \n" +
			"------------------/---------__---__---__----__----__----__-/---)__----__- \n" +
			"                 /        /   ) (_ ` (_ ` /   ) /   ) /   /   /   ) /   ) \n" +
			"________________(____/___(___(_(__)_(__)_(___(_/___/_(___/___/_____(___(_ \n" +
			"                                                                          \n" +
			"                                                                          \n" +
			"__________________________________________________________________________\n" +
			"                                                     __                   \n" +
			"                                                   /    )   ,         /   \n" +
			"---------------------------------------------------\\------------__---/-__-\n" +
			"                                                    \\     /   /   ) /(    \n" +
			"________________________________________________(____/___/___/___/_/___\\__\n" +
			"\n\t\tby A2 Rešitve d.o.o.";

	private CassandraSinkConnectorConfig config;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		config = new CassandraSinkConnectorConfig(props);
		if ((StringUtils.equals(config.getString(ParamConstants.SECURITY_PARAM), ParamConstants.SECURITY_PASSWORD) ||
			StringUtils.equals(config.getString(ParamConstants.SECURITY_PARAM), ParamConstants.SECURITY_AWS_PASSWORD)) &&
				(StringUtils.isBlank(config.getString(ParamConstants.USERNAME_PARAM)) ||
				StringUtils.isBlank(config.getPassword(ParamConstants.PASSWORD_PARAM).value()))) {
			LOGGER.error("When {} is set to {} parameters {} and {} must be set valid values!",
					ParamConstants.SECURITY_PARAM, config.getString(ParamConstants.SECURITY_PARAM),
					ParamConstants.USERNAME_PARAM, ParamConstants.PASSWORD_PARAM);
			throw new ConnectException("Connection parameters are not properly set!");
		}
		LOGGER.info(LOGO);
	}

	@Override
	public void stop() {
	}

	@Override
	public Class<? extends Task> taskClass() {
		return CassandraSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// Only one task currently!
		final List<Map<String, String>> configs = new ArrayList<>();
		final Map<String, String> props = new HashMap<>();
		props.putAll(config.originalsStrings());
		config.values().forEach((k, v) -> {
			if (v instanceof Boolean) {
				props.put(k, ((Boolean) v).toString());
			} else if (v instanceof Short) {
				props.put(k, ((Short) v).toString());
			} else if (v instanceof Integer) {
				props.put(k, ((Integer) v).toString());
			} else if (v instanceof Long) {
				props.put(k, ((Long) v).toString());
			} else if (v instanceof Password) {
				props.put(k, ((Password) v).value());
			} else if (v instanceof List) {
				if (v != null &&
						((List<?>)v).size() > 0 &&
						((List<?>)v).get(0) instanceof String) {
					props.put(k, StringUtils.join((List<?>) v, ","));
				}
			} else {
				//TODO - need to handle more types
				props.put(k, (String) v);
			}
		});
		configs.add(props);
		return configs;
	}

	@Override
	public ConfigDef config() {
		return CassandraSinkConnectorConfig.config();
	}

}
