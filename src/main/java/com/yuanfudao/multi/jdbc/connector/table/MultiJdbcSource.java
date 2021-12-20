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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

/** multi jdbc source. */
public class MultiJdbcSource
        implements Source<RowData, MultiJdbcPartitionSplit, MultiJdbcSourceEnumState> {

    private final MultiJdbcOptions options;
    private final MultiJdbcReadOptions readOptions;
    private final RowType rowType;
    private final String[] columns;
    private final ReadableConfig config;
    private final TypeInformation<RowData> typeInformation;

    public MultiJdbcSource(
            MultiJdbcOptions options,
            MultiJdbcReadOptions readOptions,
            ResolvedSchema schema,
            ReadableConfig config,
            TypeInformation<RowData> typeInformation) {
        this.options = options;
        this.readOptions = readOptions;
        this.config = config;
        this.typeInformation = typeInformation;
        this.rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        this.columns = schema.getColumnNames().toArray(new String[0]);
    }

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /**
     * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
     * and does not have any state to resume.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return A new SourceReader.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause task failure/recovery.
     */
    @Override
    public SourceReader<RowData, MultiJdbcPartitionSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        return new MultiJdbcReader(
                options,
                readOptions,
                rowType,
                typeInformation,
                (Configuration) config,
                readerContext);
    }

    /**
     * Creates a new SplitEnumerator for this source, starting a new input.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return A new SplitEnumerator.
     * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    @Override
    public SplitEnumerator<MultiJdbcPartitionSplit, MultiJdbcSourceEnumState> createEnumerator(
            SplitEnumeratorContext<MultiJdbcPartitionSplit> enumContext) throws Exception {
        return new MultiJdbcEnumerator(options, readOptions, columns, enumContext);
    }

    /**
     * Restores an enumerator from a checkpoint.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return A SplitEnumerator restored from the given checkpoint.
     * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
     *     thrown from this method cause JobManager failure/recovery.
     */
    @Override
    public SplitEnumerator<MultiJdbcPartitionSplit, MultiJdbcSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<MultiJdbcPartitionSplit> enumContext,
            MultiJdbcSourceEnumState checkpoint)
            throws Exception {
        // 恢复就是从头拉取
        return new MultiJdbcEnumerator(options, readOptions, columns, enumContext);
    }

    /**
     * Creates a serializer for the source splits. Splits are serialized when sending them from
     * enumerator to reader, and when checkpointing the reader's current state.
     *
     * @return The serializer for the split type.
     */
    @Override
    public SimpleVersionedSerializer<MultiJdbcPartitionSplit> getSplitSerializer() {
        return new MultiJdbcSplitSerializer();
    }

    /**
     * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
     * the result of the {@link SplitEnumerator#snapshotState()} method.
     *
     * @return The serializer for the SplitEnumerator checkpoint.
     */
    @Override
    public SimpleVersionedSerializer<MultiJdbcSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new MultiJdbcSourceEnumStateSerializer();
    }
}
