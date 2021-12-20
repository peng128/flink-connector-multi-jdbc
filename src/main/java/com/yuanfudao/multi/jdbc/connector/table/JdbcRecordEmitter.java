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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

/** jdbc record emitter. */
public class JdbcRecordEmitter
        implements RecordEmitter<RecordAndPosition<RowData>, RowData, MultiJdbcSplitState> {
    /**
     * Process and emit the records to the {@link SourceOutput}. A few recommendations to the
     * implementation are following:
     *
     * <ul>
     *   <li>The method maybe interrupted in the middle. In that case, the same set of records will
     *       be passed to the record emitter again later. The implementation needs to make sure it
     *       reades
     *   <li>
     * </ul>
     *
     * @param element The intermediate element read by the SplitReader.
     * @param output The output to which the final records are emit to.
     * @param splitState The state of the split.
     */
    @Override
    public void emitRecord(
            RecordAndPosition<RowData> element,
            SourceOutput<RowData> output,
            MultiJdbcSplitState splitState)
            throws Exception {
        output.collect(element.getRecord());
    }
}
