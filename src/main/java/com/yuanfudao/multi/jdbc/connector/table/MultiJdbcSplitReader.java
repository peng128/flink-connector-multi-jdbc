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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/** split reader for multi jdbc. */
public class MultiJdbcSplitReader
        implements SplitReader<RecordAndPosition<RowData>, MultiJdbcPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiJdbcSplitReader.class);

    private InputFormatReader inputFormatReader;
    private final MultiJdbcOptions options;
    private final MultiJdbcReadOptions readOptions;
    private final RowType rowType;
    private final TypeInformation<RowData> typeInformation;

    @Nullable private String currentSplitId;

    private final Queue<MultiJdbcPartitionSplit> splits;

    public MultiJdbcSplitReader(
            MultiJdbcOptions multiJdbcOptions,
            MultiJdbcReadOptions readOptions,
            RowType rowType,
            TypeInformation<RowData> typeInformation) {
        this.options = multiJdbcOptions;
        this.readOptions = readOptions;
        this.rowType = rowType;
        this.typeInformation = typeInformation;
        this.splits = new ArrayDeque<>();
    }

    /**
     * Fetch elements into the blocking queue for the given splits. The fetch call could be blocking
     * but it should get unblocked when {@link #wakeUp()} is invoked. In that case, the
     * implementation may either decide to return without throwing an exception, or it can just
     * throw an interrupted exception. In either case, this method should be reentrant, meaning that
     * the next fetch call should just resume from where the last fetch call was waken up or
     * interrupted.
     *
     * @return the Ids of the finished splits.
     * @throws IOException when encountered IO errors, such as deserialization failures.
     */
    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> fetch() throws IOException {
        if (inputFormatReader == null) {
            final MultiJdbcPartitionSplit nextSplit = splits.poll();
            if (nextSplit == null) {
                throw new IOException("Cannot fetch from another split - no split remaining");
            }

            currentSplitId = nextSplit.splitId();

            MultiJdbcInputFormatReader.Builder inputFormatReaderBuilder =
                    new MultiJdbcInputFormatReader.Builder()
                            .setAutoCommit(readOptions.getAutoCommit())
                            .setDBUrl(nextSplit.getJdbcUrl())
                            .setDrivername(options.getDriverName())
                            .setQuery(nextSplit.getQuery())
                            .setRowConverter(options.getDialect().getRowConverter(rowType))
                            .setRowDataTypeInfo(typeInformation);

            if (readOptions.getFetchSize() != 0) {
                inputFormatReaderBuilder.setFetchSize(readOptions.getFetchSize());
            }

            if (options.getUsername().isPresent() && options.getPassword().isPresent()) {
                inputFormatReaderBuilder
                        .setUsername(options.getUsername().get())
                        .setPassword(options.getPassword().get());
            }
            inputFormatReader = inputFormatReaderBuilder.build();
            inputFormatReader.openInputFormat();
        }

        if (!inputFormatReader.reachedEnd()) {
            return JdbcRowDataRecord.forRecord(currentSplitId, inputFormatReader);
        } else {
            inputFormatReader.closeInputFormat();
            inputFormatReader.close();
            return JdbcRowDataRecord.finishSplit(currentSplitId);
        }
    }

    /**
     * Handle the split changes. This call should be non-blocking.
     *
     * @param splitsChanges the split changes that the SplitReader needs to handle.
     */
    @Override
    public void handleSplitsChanges(SplitsChange<MultiJdbcPartitionSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    /** Wake up the split reader in case the fetcher thread is blocking in {@link #fetch()}. */
    @Override
    public void wakeUp() {}

    /**
     * Close the split reader.
     *
     * @throws Exception if closing the split reader failed.
     */
    @Override
    public void close() throws Exception {
        if (inputFormatReader != null) {
            inputFormatReader.closeInputFormat();
            inputFormatReader.close();
        }
    }
}
