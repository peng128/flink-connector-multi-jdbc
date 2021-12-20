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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** multi jdbc input format reader. */
public class MultiJdbcInputFormatReader implements InputFormatReader {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(MultiJdbcInputFormatReader.class);

    private JdbcConnectionProvider connectionProvider;
    private int fetchSize;
    private Boolean autoCommit;
    private String queryTemplate;
    private int resultSetType;
    private int resultSetConcurrency;
    private JdbcRowConverter rowConverter;
    private TypeInformation<RowData> rowDataTypeInfo;

    private transient PreparedStatement statement;
    private transient ResultSet resultSet;
    private transient boolean hasNext;

    private MultiJdbcInputFormatReader(
            JdbcConnectionProvider connectionProvider,
            int fetchSize,
            Boolean autoCommit,
            String queryTemplate,
            int resultSetType,
            int resultSetConcurrency,
            JdbcRowConverter rowConverter,
            TypeInformation<RowData> rowDataTypeInfo) {
        this.connectionProvider = connectionProvider;
        this.fetchSize = fetchSize;
        this.autoCommit = autoCommit;
        this.queryTemplate = queryTemplate;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.rowConverter = rowConverter;
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public void openInputFormat() {
        // called once per inputFormat (on open)
        try {
            Connection dbConn = connectionProvider.getOrEstablishConnection();
            // set autoCommit mode only if it was explicitly configured.
            // keep connection default otherwise.
            if (autoCommit != null) {
                dbConn.setAutoCommit(autoCommit);
            }
            statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
            if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                    "JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
        }
    }

    @Override
    public void closeInputFormat() {
        // called once per inputFormat (on close)
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
        } finally {
            statement = null;
        }

        connectionProvider.closeConnection();
    }

    /**
     * Closes all resources used.
     *
     * @throws IOException Indicates that a resource could not be closed.
     */
    @Override
    public void close() throws IOException {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    /**
     * Checks whether all data has been read.
     *
     * @return boolean value indication whether all data has been read.
     * @throws IOException
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    /**
     * Stores the next resultSet row in a tuple.
     *
     * @return row containing next {@link RowData}
     * @throws IOException
     */
    @Override
    public RowData nextRecord() throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            RowData row = rowConverter.toInternal(resultSet);
            // update hasNext after we've read the record
            hasNext = resultSet.next();
            return row;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    /** Builder for {@link JdbcRowDataInputFormat}. */
    public static class Builder {
        private JdbcConnectionOptions.JdbcConnectionOptionsBuilder connOptionsBuilder;
        private int fetchSize;
        private Boolean autoCommit;
        private String queryTemplate;
        private JdbcRowConverter rowConverter;
        private TypeInformation<RowData> rowDataTypeInfo;
        private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

        public Builder() {
            this.connOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        }

        public Builder setDrivername(String drivername) {
            this.connOptionsBuilder.withDriverName(drivername);
            return this;
        }

        public Builder setDBUrl(String dbURL) {
            this.connOptionsBuilder.withUrl(dbURL);
            return this;
        }

        public Builder setUsername(String username) {
            this.connOptionsBuilder.withUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            this.connOptionsBuilder.withPassword(password);
            return this;
        }

        public Builder setQuery(String query) {
            this.queryTemplate = query;
            return this;
        }

        public Builder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public Builder setRowConverter(JdbcRowConverter rowConverter) {
            this.rowConverter = rowConverter;
            return this;
        }

        public Builder setFetchSize(int fetchSize) {
            Preconditions.checkArgument(
                    fetchSize == Integer.MIN_VALUE || fetchSize > 0,
                    "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.",
                    fetchSize);
            this.fetchSize = fetchSize;
            return this;
        }

        public Builder setAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder setResultSetType(int resultSetType) {
            this.resultSetType = resultSetType;
            return this;
        }

        public Builder setResultSetConcurrency(int resultSetConcurrency) {
            this.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

        public MultiJdbcInputFormatReader build() {
            if (this.queryTemplate == null) {
                throw new NullPointerException("No query supplied");
            }
            if (this.rowConverter == null) {
                throw new NullPointerException("No row converter supplied");
            }
            return new MultiJdbcInputFormatReader(
                    new SimpleJdbcConnectionProvider(connOptionsBuilder.build()),
                    this.fetchSize,
                    this.autoCommit,
                    this.queryTemplate,
                    this.resultSetType,
                    this.resultSetConcurrency,
                    this.rowConverter,
                    this.rowDataTypeInfo);
        }
    }
}
