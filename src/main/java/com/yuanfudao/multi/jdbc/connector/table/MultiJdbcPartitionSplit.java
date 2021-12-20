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

package com.yuanfudao.multi.jdbc.connector.table;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/** partition split for multi jdbc. */
public class MultiJdbcPartitionSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String query;

    private final String jdbcUrl;

    private final String username;

    private final String password;

    public MultiJdbcPartitionSplit(String query, String jdbcUrl, String username, String password) {
        this.query = query;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public String getQuery() {
        return query;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    @Override
    public String splitId() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiJdbcPartitionSplit split = (MultiJdbcPartitionSplit) o;
        return Objects.equals(query, split.query)
                && Objects.equals(jdbcUrl, split.jdbcUrl)
                && Objects.equals(username, split.username)
                && Objects.equals(password, split.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, jdbcUrl, username, password);
    }

    @Override
    public String toString() {
        return "MultiJdbcPartitionSplit{"
                + "query='"
                + query
                + '\''
                + ", jdbcUrl='"
                + jdbcUrl
                + '\''
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
