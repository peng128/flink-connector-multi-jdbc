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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

import java.util.Map;

/** multi jdbc reader. */
public class MultiJdbcReader
        extends SingleThreadMultiplexSourceReaderBase<
                RecordAndPosition<RowData>, RowData, MultiJdbcPartitionSplit, MultiJdbcSplitState> {

    public MultiJdbcReader(
            MultiJdbcOptions multiJdbcOptions,
            MultiJdbcReadOptions readOptions,
            RowType rowType,
            TypeInformation<RowData> typeInformation,
            Configuration config,
            SourceReaderContext context) {
        super(
                () ->
                        new MultiJdbcSplitReader(
                                multiJdbcOptions, readOptions, rowType, typeInformation),
                new JdbcRecordEmitter(),
                config,
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    /**
     * When new splits are added to the reader. The initialize the state of the new splits.
     *
     * @param split a newly added split.
     */
    @Override
    protected MultiJdbcSplitState initializedState(MultiJdbcPartitionSplit split) {
        return new MultiJdbcSplitState(split);
    }

    /**
     * Convert a mutable SplitStateT to immutable SplitT.
     *
     * @param splitId
     * @param splitState splitState.
     * @return an immutable Split state.
     */
    @Override
    protected MultiJdbcPartitionSplit toSplitType(String splitId, MultiJdbcSplitState splitState) {
        return splitState.getSplit();
    }

    /**
     * Handles the finished splits to clean the state if needed.
     *
     * @param finishedSplitIds
     */
    @Override
    protected void onSplitFinished(Map finishedSplitIds) {
        context.sendSplitRequest();
    }
}
