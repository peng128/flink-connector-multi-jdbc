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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/** jdbc row data record for multi jdbc. */
public class JdbcRowDataRecord implements RecordsWithSplitIds<RecordAndPosition<RowData>> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataRecord.class);

    @Nullable private String splitId;

    private final Set<String> finishedSplits;

    private final InputFormatReader inputFormatReader;

    private InputFormatReader currentReader;

    long count = 0;

    public JdbcRowDataRecord(
            @Nullable String splitId,
            Set<String> finishedSplits,
            InputFormatReader inputFormatReader) {
        this.splitId = splitId;
        this.finishedSplits = finishedSplits;
        this.inputFormatReader = inputFormatReader;
    }

    /**
     * Moves to the next split. This method is also called initially to move to the first split.
     * Returns null, if no splits are left.
     */
    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;
        count = 0;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.currentReader = nextSplit != null ? this.inputFormatReader : null;

        return nextSplit;
    }

    /**
     * Gets the next record from the current split. Returns null if no more records are left in this
     * split.
     */
    @Nullable
    @Override
    public RecordAndPosition<RowData> nextRecordFromSplit() {
        try {
            if (!currentReader.reachedEnd()) {
                return new RecordAndPosition<>(currentReader.nextRecord(), count++, 0);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            LOG.error(Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Get the finished splits.
     *
     * @return the finished splits after this RecordsWithSplitIds is returned.
     */
    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }

    public static JdbcRowDataRecord forRecord(
            final String splitId, final InputFormatReader inputFormatReader) {
        return new JdbcRowDataRecord(splitId, Collections.emptySet(), inputFormatReader);
    }

    public static JdbcRowDataRecord finishSplit(final String splitId) {
        return new JdbcRowDataRecord(null, Collections.singleton(splitId), null);
    }
}
