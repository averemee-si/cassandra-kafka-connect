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

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration routines
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraConfigUtils {

	private static final String PREFIX = "DATASTAX__JAVA__DRIVER_";
	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConfigUtils.class);

	public static void configure() {
		boolean loadConfFromEnv = false;
		Class<CassandraConfigUtils> clazz = CassandraConfigUtils.class;
		try {
			if (clazz.getResourceAsStream("/application.conf") == null &&
				clazz.getResourceAsStream("/application.json") == null &&
				clazz.getResourceAsStream("/application.properties") == null) {
				loadConfFromEnv = true;
			}
		} catch (Exception e) {
			loadConfFromEnv = true;
		}
		if (loadConfFromEnv) {
			final Map<String, String> env = System.getenv();
			env.forEach((variable, value) -> {
				if (variable.startsWith(PREFIX)) {
					final String property = "datastax-java-driver." +
							variable
								.substring(PREFIX.length())
								.replaceAll("___", "%%%")
								.replaceAll("__", "-")
								.replaceAll("_", ".")
								.replaceAll("%%%", "_")
								.toLowerCase();
					LOGGER.info("Setting DataStax Java Driver parameter '{}' to '{}'",
							property, value);
					System.setProperty(property, value);
				}
			});
		}
		System.setProperty("datastax-java-driver.basic.session-name", "a2-cassandra-connector");
	}

	public static void configure4Aws(final CassandraSinkConnectorConfig config) {
		setupSsl4Aws("/aws_keyspaces_truststore.jks", "cassandra");
		System.setProperty("datastax-java-driver.advanced.ssl-engine-factory.class", "DefaultSslEngineFactory");
		System.setProperty("datastax-java-driver.advanced.ssl-engine-factory.hostname-validation", "false");
		final String authType = config.getString(ParamConstants.SECURITY_PARAM);
		if (StringUtils.equals(authType, ParamConstants.SECURITY_AWS_PASSWORD)) {
			System.setProperty("datastax-java-driver.advanced.auth-provider.class",
					"PlainTextAuthProvider");
			System.setProperty("datastax-java-driver.advanced.auth-provider.username",
					config.getString(ParamConstants.USERNAME_PARAM));
			System.setProperty("datastax-java-driver.advanced.auth-provider.password",
					config.getPassword(ParamConstants.PASSWORD_PARAM).value());
		} else {
			// SECURITY_AWS_SIGV4
			System.setProperty("datastax-java-driver.advanced.auth-provider.class",
					"software.aws.mcs.auth.SigV4AuthProvider");
			System.setProperty("datastax-java-driver.advanced.auth-provider.aws-region",
					config.getString(ParamConstants.LOCAL_DATACENTER_PARAM));
		}
	}

	private static void setupSsl4Aws(final String trustStoreResource, final String password) {
		InputStream inputStream = CassandraConfigUtils.class.getResourceAsStream(trustStoreResource);
		if (inputStream != null) {
			try {
				KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
				trustStore.load(inputStream, password.toCharArray());
				TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());    
				trustFactory.init(trustStore);
				TrustManager[] trustManagers = trustFactory.getTrustManagers();

				SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustManagers, null);
				SSLContext.setDefault(sslContext);
			} catch (KeyStoreException |
						NoSuchAlgorithmException |
						CertificateException |
						IOException | KeyManagementException e) {
				throw new ConnectException(e);
			}
		} else {
			throw new ConnectException("Unable to find required '" + trustStoreResource + "' in connector's jar file!");
		}
	}

	public static void main(String[] argv) {
		configure4Aws(null);
	}
}
