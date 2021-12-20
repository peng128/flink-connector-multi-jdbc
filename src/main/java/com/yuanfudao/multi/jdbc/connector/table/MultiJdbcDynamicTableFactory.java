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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Factory for creating configured instances of {@link JdbcDynamicTableSource}. */
public class MultiJdbcDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "multi-jdbc";
    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC database URL.");
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC table name(support regex).");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC schema name(support regex).");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The JDBC password.");
    private static final ConfigOption<String> DRIVER =
            ConfigOptions.key("driver")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The class name of the JDBC driver to use to connect to this URL. "
                                    + "If not set, it will automatically be derived from the URL.");
    public static final ConfigOption<Duration> MAX_RETRY_TIMEOUT =
            ConfigOptions.key("connection.max-retry-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("Maximum timeout between retries.");

    private static final ConfigOption<String> SCAN_PARTITION_COLUMN =
            ConfigOptions.key("scan.partition.column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The column name used for partitioning the input.");
    private static final ConfigOption<Integer> SCAN_BATCH_SIZE =
            ConfigOptions.key("scan.batch.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of partitions.");
    private static final ConfigOption<Integer> SCAN_PARTITION_NUM =
            ConfigOptions.key("scan.partition.num")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The number of partitions for one table.");

    /**
     * Creates a {@link DynamicTableSource} instance from a {@link CatalogTable} and additional
     * context information.
     *
     * <p>An implementation should perform validation and the discovery of further (nested)
     * factories in this method.
     *
     * @param context
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        context.getConfiguration();
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        return new MultiJdbcDynamicTableSource(
                getJdbcOptions(helper.getOptions()),
                getJdbcReadOptions(helper.getOptions()),
                context.getCatalogTable().getResolvedSchema(),
                config);
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory requires in
     * addition to {@link #optionalOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(SCHEMA_NAME);
        return requiredOptions;
    }

    /**
     * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in
     * addition to {@link #requiredOptions()}.
     *
     * <p>See the documentation of {@link Factory} for more information.
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DRIVER);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_BATCH_SIZE);
        optionalOptions.add(SCAN_PARTITION_NUM);
        optionalOptions.add(MAX_RETRY_TIMEOUT);
        return optionalOptions;
    }

    private MultiJdbcOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final MultiJdbcOptions.Builder builder =
                MultiJdbcOptions.builder()
                        .setDBUrl(url)
                        .setTableName(readableConfig.get(TABLE_NAME))
                        .setSchemaName(readableConfig.get(SCHEMA_NAME))
                        .setDialect(JdbcDialects.get(url).get())
                        .setConnectionCheckTimeoutSeconds(
                                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

        readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private MultiJdbcReadOptions getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName =
                readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        final MultiJdbcReadOptions.Builder builder = MultiJdbcReadOptions.builder();
        if (partitionColumnName.isPresent()) {
            builder.setPartitionColumnName(partitionColumnName.get());
            final Optional<Integer> batchSize = readableConfig.getOptional(SCAN_BATCH_SIZE);
            final Optional<Integer> partitionNum = readableConfig.getOptional(SCAN_PARTITION_NUM);
            if (!batchSize.isPresent() && !partitionNum.isPresent()) {
                throw new RuntimeException("neither batch size or partition num is set");
            } else if (batchSize.isPresent() && partitionNum.isPresent()) {
                throw new RuntimeException("batch size and partition num can not appear currently");
            }
            partitionNum.ifPresent(builder::setNumPartitions);
            batchSize.ifPresent(builder::setBatchSize);
        }
        return builder.build();
    }
}
