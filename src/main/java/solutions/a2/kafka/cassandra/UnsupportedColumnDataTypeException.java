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

/**
 * UnsupportedColumnDataTypeException
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class UnsupportedColumnDataTypeException extends Exception {

	private static final long serialVersionUID = -5774328960552655145L;

	private final String columnName;

	public UnsupportedColumnDataTypeException(final String columnName) {
		super();
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

}
