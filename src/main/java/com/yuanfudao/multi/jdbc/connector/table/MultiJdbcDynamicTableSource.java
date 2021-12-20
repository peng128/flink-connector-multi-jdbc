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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

/** dynamic table source for multi jdbc connector. */
public class MultiJdbcDynamicTableSource implements ScanTableSource {
    private final MultiJdbcOptions options;
    private final MultiJdbcReadOptions readOptions;
    private ResolvedSchema physicalSchema;
    private final ReadableConfig config;

    public MultiJdbcDynamicTableSource(
            MultiJdbcOptions options,
            MultiJdbcReadOptions readOptions,
            ResolvedSchema physicalSchema,
            ReadableConfig config) {
        this.options = options;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
        this.config = config;
    }

    /**
     * Returns the set of changes that the planner can expect during runtime.
     *
     * @see RowKind
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a source
     * implementation emits internal data structures (see {@link RowData} for more information).
     *
     * <p>The given {@link ScanContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>See {@code org.apache.flink.table.connector.source.SourceFunctionProvider} in {@code
     * flink-table-api-java-bridge}.
     *
     * @param runtimeProviderContext
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {

            @Override
            public boolean isBounded() {
                return true;
            }

            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                MultiJdbcSource multiJdbcSource =
                        new MultiJdbcSource(
                                options,
                                readOptions,
                                physicalSchema,
                                config,
                                runtimeProviderContext.createTypeInformation(
                                        physicalSchema.toPhysicalRowDataType()));
                return execEnv.fromSource(
                        multiJdbcSource,
                        WatermarkStrategy.noWatermarks(),
                        String.format(
                                "multi jdbc source : [%s].[%s]",
                                options.getSchemaName(), options.getTableName()));
            }
        };
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSource copy() {
        return new MultiJdbcDynamicTableSource(options, readOptions, physicalSchema, config);
    }

    /** Returns a string that summarizes this source for printing to a console or log. */
    @Override
    public String asSummaryString() {
        return String.format(
                "multi jdbc source : [%s].[%s]", options.getSchemaName(), options.getTableName());
    }
}
