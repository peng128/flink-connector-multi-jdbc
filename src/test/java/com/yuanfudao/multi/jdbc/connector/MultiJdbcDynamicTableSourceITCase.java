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

package com.yuanfudao.multi.jdbc.connector;

import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/** ITCase for {@link JdbcDynamicTableSource}. */
public class MultiJdbcDynamicTableSourceITCase extends AbstractTestBase {

    public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DB_URL_1 = "jdbc:derby:memory:test1";
    public static final String DB_URL_2 = "jdbc:derby:memory:test2";
    public static final String TABLE_NAME = "jdbc_source";
    public static final String INPUT_TABLE = "JDBCSOURCE.*";
    public static final String INPUT_DB = "APP";
    public static final String INPUT_TABLE_1 = "jdbcSource_1";
    public static final String INPUT_TABLE_2 = "jdbcSource_2";
    public static final String INPUT_TABLE_3 = "other_3";

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, envSettings);

        System.setProperty(
                "derby.stream.error.field", MultiJdbcDynamicTableSourceITCase.class.getCanonicalName() + ".DEV_NULL");
        Class.forName(DRIVER_CLASS);

        initTable(DB_URL_1);
        initTable(DB_URL_2);
    }

    private void initTable(String url) throws ClassNotFoundException, SQLException {
        try (Connection conn = DriverManager.getConnection(url + ";create=true");
                Statement statement = conn.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE_1
                            + " (id BIGINT NOT NULL,"
                            + "context VARCHAR(255))");
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE_2
                            + " ("
                            + "id BIGINT NOT NULL,"
                            + "context VARCHAR(255))");
            statement.executeUpdate(
                    "CREATE TABLE "
                            + INPUT_TABLE_3
                            + " ("
                            + "id BIGINT NOT NULL,"
                            + "context_other VARCHAR(255))");
            statement.executeUpdate(
                    "INSERT INTO " + INPUT_TABLE_1 + " VALUES (" + "1, 'a'),(2,'b'),(3,'c')");
            statement.executeUpdate(
                    "INSERT INTO " + INPUT_TABLE_2 + " VALUES (" + "4, 'a1'),(5,'b1'),(6,'c1')");
            statement.executeUpdate(
                    "INSERT INTO " + INPUT_TABLE_3 + " VALUES (" + "7, 'a2'),(8,'b2'),(9,'c2')");
        }
    }

    private void dropTable(String url) throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
                Statement stat = conn.createStatement()) {
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE_1);
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE_2);
            stat.executeUpdate("DROP TABLE " + INPUT_TABLE_3);
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(DRIVER_CLASS);
        dropTable(DB_URL_1);
        dropTable(DB_URL_2);
        StreamTestSink.clear();
    }

    @Test
    public void testJdbcSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + TABLE_NAME
                        + "("
                        + "id BIGINT NOT NULL,"
                        + "context VARCHAR(255)"
                        + ") WITH ("
                        + "  'connector'='multi-jdbc',"
                        + "  'url'='"
                        + String.format("%s;%s", DB_URL_1, DB_URL_2)
                        + "',"
                        + "  'table-name'='"
                        + INPUT_TABLE
                        + "',"
                        + "  'schema-name'='"
                        + INPUT_DB
                        + "',"
                        + "'scan.partition.column' = 'id',"
                        + "  'scan.batch.size' = '2'"
                        + ")");

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + TABLE_NAME).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                                "+I[1, a]",
                                "+I[1, a]",
                                "+I[2, b]",
                                "+I[2, b]",
                                "+I[3, c]",
                                "+I[3, c]",
                                "+I[4, a1]",
                                "+I[4, a1]",
                                "+I[5, b1]",
                                "+I[5, b1]",
                                "+I[6, c1]",
                                "+I[6, c1]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }
}
