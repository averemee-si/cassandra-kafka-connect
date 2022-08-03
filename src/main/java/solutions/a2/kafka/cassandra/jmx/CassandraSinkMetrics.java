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

package solutions.a2.kafka.cassandra.jmx;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.ExceptionUtils;

/**
 * Cassandra Sink Connector Metrics MBean implementation
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class CassandraSinkMetrics implements CassandraSinkMetricsMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSinkMetrics.class);
	private static final String DURATION_FMT = "%sdays %shrs %smin %ssec.\n";

	private long startTimeMillis;
	private LocalDateTime startTime;
	private long upsertBindNanos;
	private long upsertExecNanos;
	private long upsertCount;
	private long deleteBindNanos;
	private long deleteExecNanos;
	private long deleteCount;

	public CassandraSinkMetrics(final String connectorName, final String keyspaceName, final String tableName) {
		startTimeMillis = System.currentTimeMillis();
		startTime = LocalDateTime.now();
		upsertBindNanos = 0;
		upsertExecNanos = 0;
		upsertCount = 0;
		deleteBindNanos = 0;
		deleteExecNanos = 0;
		deleteCount = 0;

		final StringBuilder sb = new StringBuilder(256);
		sb.append("solutions.a2.kafka:type=Cassandra-Sink-Connector-metrics,name=");
		sb.append(connectorName);
		sb.append(",keyspace=");
		sb.append(keyspaceName);
		sb.append(",table=");
		sb.append(tableName);
		try {
			final ObjectName name = new ObjectName(sb.toString());
			final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			if (mbs.isRegistered(name)) {
				LOGGER.warn("JMX MBean {} already registered, trying to remove it.", name.getCanonicalName());
				try {
					mbs.unregisterMBean(name);
				} catch (InstanceNotFoundException nfe) {
					LOGGER.error("Unable to unregister MBean {}", name.getCanonicalName());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(nfe));
					throw new ConnectException(nfe);
				}
			}
			mbs.registerMBean(this, name);
			LOGGER.debug("MBean {} registered.", sb.toString());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", sb.toString());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	private static String formatDuration(final Duration duration) {
		return String.format(DURATION_FMT,
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}

	public void addMetrics(final boolean isUpsert, final long bindNanos, final long execNanos) {
		if (isUpsert) {
			upsertBindNanos += bindNanos;
			upsertExecNanos += execNanos;
			upsertCount++;
		} else {
			deleteBindNanos += bindNanos;
			deleteExecNanos += execNanos;
			deleteCount++;
		}
	}

	@Override
	public String getStartTime() {
		return startTime.format(DateTimeFormatter.ISO_DATE_TIME);
	}
	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}
	@Override
	public String getElapsedTime() {
		final Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return formatDuration(duration);
	}

	@Override
	public long getTotalRecordsCount() {
		return upsertCount + deleteCount;
	}

	@Override
	public long getUpsertRecordsCount() {
		return upsertCount;
	}
	@Override
	public long getUpsertBindMillis() {
		return upsertBindNanos / 1_000_000;
	}
	@Override
	public String getUpsertBindTime() {
		final Duration duration = Duration.ofNanos(upsertBindNanos);
		return formatDuration(duration);
	}
	@Override
	public long getUpsertExecMillis() {
		return upsertExecNanos / 1_000_000;
	}
	@Override
	public String getUpsertExecTime() {
		final Duration duration = Duration.ofNanos(upsertExecNanos);
		return formatDuration(duration);
	}
	@Override
	public long getUpsertTotalMillis() {
		return (upsertBindNanos + upsertExecNanos) / 1_000_000;
	}
	@Override
	public String getUpsertTotalTime() {
		final Duration duration = Duration.ofNanos(upsertBindNanos + upsertExecNanos);
		return formatDuration(duration);
	}
	@Override
	public int getUpsertPerSecond() {
		if (upsertCount == 0) {
			return 0;
		} else if ((upsertBindNanos + upsertExecNanos) == 0) {
			return Integer.MAX_VALUE;
		} else {
			return (int) Math.round((upsertCount * 1_000_000_000) / (upsertBindNanos + upsertExecNanos));
		}
	}

	@Override
	public long getDeleteRecordsCount() {
		return deleteCount;
	}
	@Override
	public long getDeleteBindMillis() {
		return deleteBindNanos / 1_000_000;
	}
	@Override
	public String getDeleteBindTime() {
		final Duration duration = Duration.ofNanos(deleteBindNanos);
		return formatDuration(duration);
	}
	@Override
	public long getDeleteExecMillis() {
		return deleteExecNanos / 1_000_000;
	}
	@Override
	public String getDeleteExecTime() {
		final Duration duration = Duration.ofNanos(deleteExecNanos);
		return formatDuration(duration);
	}
	@Override
	public long getDeleteTotalMillis() {
		return (deleteBindNanos + deleteExecNanos) / 1_000_000;
	}
	@Override
	public String getDeleteTotalTime() {
		final Duration duration = Duration.ofNanos(deleteBindNanos + deleteExecNanos);
		return formatDuration(duration);
	}
	@Override
	public int getDeletePerSecond() {
		if (deleteCount == 0) {
			return 0;
		} else if ((deleteBindNanos + deleteExecNanos) == 0) {
			return Integer.MAX_VALUE;
		} else {
			return (int) Math.round((deleteCount * 1_000_000_000) / (deleteBindNanos + deleteExecNanos));
		}
	}

}
