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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** enumerator for multi jdbc. */
public class MultiJdbcEnumerator
        implements SplitEnumerator<MultiJdbcPartitionSplit, MultiJdbcSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(MultiJdbcEnumerator.class.getName());

    private final MultiJdbcOptions options;
    private final MultiJdbcReadOptions readOptions;
    private final String[] columns;
    private final SplitEnumeratorContext<MultiJdbcPartitionSplit> context;
    private final Deque<MultiJdbcPartitionSplit> splitsQueue = new ArrayDeque<>();

    private static final String SELECT_COUNT = "select count(*) from %s";
    private static final String USE_DATABASE = "use %s;";
    private static final String SELECT_MAX_MIN = "select max(%s), min(%s) from %s";

    private final AtomicInteger jdbcUrlCounter = new AtomicInteger(0);
    private final AtomicInteger totalSchemaCounter = new AtomicInteger(0);
    private final AtomicInteger totalTableCounter = new AtomicInteger(0);
    private final AtomicInteger queryCounter = new AtomicInteger(0);

    public MultiJdbcEnumerator(
            MultiJdbcOptions options,
            MultiJdbcReadOptions readOptions,
            String[] columns,
            SplitEnumeratorContext<MultiJdbcPartitionSplit> context) {
        this.options = options;
        this.readOptions = readOptions;
        this.columns = columns;
        this.context = context;
    }

    /**
     * Start the split enumerator.
     *
     * <p>The default behavior does nothing.
     */
    @Override
    public void start() {
        generateSplit();
    }

    /**
     * Handles the request for a split. This method is called when the reader with the given subtask
     * id calls the {@link SourceReaderContext#sendSplitRequest()} method.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param requesterHostname Optional, the hostname where the requesting task is running. This
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                    requesterHostname == null
                            ? "(no host locality info)"
                            : "(on host '" + requesterHostname + "')";
            LOG.info("Subtask {} {} is requesting a multi jdbc source split", subtaskId, hostInfo);
        }

        if (!splitsQueue.isEmpty()) {
            MultiJdbcPartitionSplit split = splitsQueue.removeFirst();
            LOG.info(
                    "assign split [{}] {} to task {}",
                    split.getJdbcUrl(),
                    split.getQuery(),
                    subtaskId);
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No more splits available for subtask {}", subtaskId);
        }
    }

    /**
     * Handles a custom source event from the source reader.
     *
     * <p>This method has a default implementation that does nothing, because it is only required to
     * be implemented by some sources, which have a custom event protocol between reader and
     * enumerator. The common events for reader registration and split requests are not dispatched
     * to this method, but rather invoke the {@link #addReader(int)} and {@link
     * #handleSplitRequest(int, String)} methods.
     *
     * @param subtaskId the subtask id of the source reader who sent the source event.
     * @param sourceEvent the source event from the source reader.
     */
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<MultiJdbcPartitionSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        for (MultiJdbcPartitionSplit split : splits) {
            splitsQueue.addLast(split);
        }
    }

    /**
     * Add a new source reader with the given subtask ID.
     *
     * @param subtaskId the subtask ID of the new source reader.
     */
    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    /**
     * Creates a snapshot of the state of this split enumerator, to be stored in a checkpoint.
     *
     * <p>The snapshot should contain the latest state of the enumerator: It should assume that all
     * operations that happened before the snapshot have successfully completed. For example all
     * splits assigned to readers via {@link SplitEnumeratorContext#assignSplit(SourceSplit, int)}
     * and {@link SplitEnumeratorContext#assignSplits(SplitsAssignment)}) don't need to be included
     * in the snapshot anymore.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshotted. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     * @throws Exception when the snapshot cannot be taken.
     */
    @Override
    public MultiJdbcSourceEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    /**
     * Called to close the enumerator, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
        // no resources to close
    }

    private void generateSplit() {
        String[] jdbcUrls = options.getDbURL().split(";");
        for (String url : jdbcUrls) {
            LOG.info("process [{}] url : {}", jdbcUrlCounter.incrementAndGet(), url);
            try (Connection conn =
                            options.getUsername().isPresent() && options.getPassword().isPresent()
                                    ? DriverManager.getConnection(
                                            url,
                                            options.getUsername().get(),
                                            options.getPassword().get())
                                    : DriverManager.getConnection(url);
                    Statement stmt = conn.createStatement()) {
                // find all tables
                Map<String, List<String>> allTables = findTables(conn);
                // gen sql query
                for (String db : allTables.keySet()) {
                    if (!options.getDialect().dialectName().equalsIgnoreCase("derby")) {
                        stmt.executeQuery(String.format(USE_DATABASE, db));
                    }
                    for (String table : allTables.get(db)) {
                        generateQuery(stmt, table, url, db);
                    }
                }
            } catch (Exception e) {
                String msg =
                        String.format(
                                "multi-jdbc enumerator generate split error: %s", e.getMessage());
                LOG.error(msg);
                LOG.error(Arrays.toString(e.getStackTrace()));
                throw new RuntimeException(e);
            }
        }
    }

    private void generateQuery(Statement stmt, String table, String url, String db)
            throws SQLException {
        // get max min
        ResultSet maxMinRes =
                stmt.executeQuery(
                        String.format(
                                SELECT_MAX_MIN,
                                readOptions.getPartitionColumnName().get(),
                                readOptions.getPartitionColumnName().get(),
                                table));
        BigInteger max = new BigInteger("0");
        BigInteger min = new BigInteger("0");
        while (maxMinRes.next()) {
            // just return if there is no data in the table
            if (maxMinRes.getString(1) == null || maxMinRes.getString(2) == null) {
                return;
            }
            max = new BigInteger(maxMinRes.getString(1));
            min = new BigInteger(maxMinRes.getString(2));
        }
        BigInteger step;
        if (readOptions.getBatchSize() != null && !readOptions.getNumPartitions().isPresent()) {
            // get count
            ResultSet countRes = stmt.executeQuery(String.format(SELECT_COUNT, table));
            long count = 0;
            while (countRes.next()) {
                count = Long.parseLong(countRes.getString(1));
            }
            // 这里如果count比分片小会引起step 超大溢出的问题，所以要判断一下
            if (readOptions.getBatchSize() > count) {
                step = max.add(min.negate());
            } else {
                step =
                        max.add(min.negate())
                                .multiply(
                                        new BigInteger(String.valueOf(readOptions.getBatchSize())))
                                .divide(new BigInteger(String.valueOf(count)));
            }
        } else if (readOptions.getBatchSize() == null
                && readOptions.getNumPartitions().isPresent()) {
            step =
                    max.add(min.negate())
                            .divide(
                                    new BigInteger(
                                            String.valueOf(
                                                    String.valueOf(
                                                            readOptions
                                                                    .getNumPartitions()
                                                                    .get()))));
        } else {
            throw new RuntimeException("neither batch size or partition num is null");
        }
        final JdbcDialect dialect = options.getDialect();
        while (min.compareTo(max) <= 0) {
            String query = getSelectFromStatement(db, table, columns, new String[0], dialect);
            query +=
                    " WHERE "
                            + dialect.quoteIdentifier(readOptions.getPartitionColumnName().get())
                            + " BETWEEN %s AND %s";
            query = String.format(query, min, min.add(step));
            if (options.getUsername().isPresent() && options.getPassword().isPresent()) {
                splitsQueue.addLast(
                        new MultiJdbcPartitionSplit(
                                query,
                                url,
                                options.getUsername().get(),
                                options.getPassword().get(),
                                queryCounter.get()));
            } else {
                splitsQueue.addLast(
                        new MultiJdbcPartitionSplit(query, url, null, null, queryCounter.get()));
            }
            min = min.add(step).add(new BigInteger("1"));
            queryCounter.incrementAndGet();
        }
    }

    private Map<String, List<String>> findTables(Connection conn) {
        Pattern schema = Pattern.compile(options.getSchemaName());
        Pattern table = Pattern.compile(options.getTableName());
        Map<String, List<String>> tables = new HashMap<>();
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet schemaResult = metaData.getCatalogs();
            if (options.getDialect().dialectName().equalsIgnoreCase("derby")) {
                schemaResult = metaData.getSchemas(null, null);
            }
            while (schemaResult.next()) {
                String schemaName = schemaResult.getString(1);
                Matcher m = schema.matcher(schemaName);
                if (m.matches()) {
                    tables.put(schemaName, new ArrayList<>());
                    LOG.info("total schema count : {}", totalSchemaCounter.incrementAndGet());
                }
            }
            for (String schemaName : tables.keySet()) {
                ResultSet tableResult = metaData.getTables(schemaName, null, null, null);
                if (options.getDialect().dialectName().equalsIgnoreCase("derby")) {
                    tableResult = metaData.getTables(null, schemaName, null, null);
                }
                while (tableResult.next()) {
                    String strTableName = tableResult.getString("TABLE_NAME");
                    Matcher m = table.matcher(strTableName);
                    if (m.matches()) {
                        tables.get(schemaName).add(strTableName);
                        LOG.info("total table counter : {}", totalTableCounter.incrementAndGet());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            LOG.error(Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e);
        }
        return tables;
    }

    @VisibleForTesting
    public Deque<MultiJdbcPartitionSplit> getSplitsQueue() {
        return splitsQueue;
    }

    /** Get select fields statement by condition fields. Default use SELECT. */
    String getSelectFromStatement(
            String schemaName, String tableName, String[] selectFields, String[] conditionFields,
            JdbcDialect jdbcDialect) {
        String selectExpressions =
                Arrays.stream(selectFields)
                        .map(jdbcDialect::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(f -> format("%s = :%s", jdbcDialect.quoteIdentifier(f), f))
                        .collect(Collectors.joining(" AND "));
        return "SELECT "
                + selectExpressions
                + " FROM "
                + jdbcDialect.quoteIdentifier(schemaName)
                + "."
                + jdbcDialect.quoteIdentifier(tableName)
                + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }
}
