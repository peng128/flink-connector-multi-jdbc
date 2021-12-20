/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yuanfudao.multi.jdbc.connector.options;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** multi jdbc options. */
public class MultiJdbcOptions extends JdbcConnectionOptions {
    private String schemaName;
    private String tableName;
    private JdbcDialect dialect;

    public MultiJdbcOptions(
            String schemaName,
            String dbURL,
            String tableName,
            String driverName,
            String username,
            String password,
            JdbcDialect dialect,
            int connectionCheckTimeoutSeconds) {
        super(dbURL, driverName, username, password, connectionCheckTimeoutSeconds);
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.dialect = dialect;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public JdbcDialect getDialect() {
        return dialect;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiJdbcOptions that = (MultiJdbcOptions) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }

    /** Builder of {@link JdbcOptions}. */
    public static class Builder {
        private String dbURL;
        private String schemaName;
        private String tableName;
        private String driverName;
        private String username;
        private JdbcDialect dialect;
        private String password;
        private int connectionCheckTimeoutSeconds = 60;

        /** required, table name. */
        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** optional, user name. */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /** optional, password. */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /** optional, connectionCheckTimeoutSeconds. */
        public Builder setConnectionCheckTimeoutSeconds(int connectionCheckTimeoutSeconds) {
            this.connectionCheckTimeoutSeconds = connectionCheckTimeoutSeconds;
            return this;
        }

        /**
         * optional, driver name, dialect has a default driver name, See {@link
         * JdbcDialect#defaultDriverName}.
         */
        public Builder setDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        /** required, JDBC DB url. */
        public Builder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        /**
         * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by {@link
         * JdbcDialects#get} from DB url.
         */
        public Builder setDialect(JdbcDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public MultiJdbcOptions build() {
            checkNotNull(dbURL, "No dbURL supplied.");
            checkNotNull(tableName, "No tableName supplied.");
            if (this.dialect == null) {
                Optional<JdbcDialect> optional = JdbcDialects.get(dbURL);
                this.dialect =
                        optional.orElseGet(
                                () -> {
                                    throw new NullPointerException(
                                            "Unknown dbURL,can not find proper dialect.");
                                });
            }
            if (this.driverName == null) {
                Optional<String> optional = dialect.defaultDriverName();
                this.driverName =
                        optional.orElseGet(
                                () -> {
                                    throw new NullPointerException("No driverName supplied.");
                                });
            }

            return new MultiJdbcOptions(
                    schemaName,
                    dbURL,
                    tableName,
                    driverName,
                    username,
                    password,
                    dialect,
                    connectionCheckTimeoutSeconds);
        }
    }
}
