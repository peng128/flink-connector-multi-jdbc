package com.yuanfudao.multi.jdbc.connector;

import com.yuanfudao.multi.jdbc.connector.table.MultiJdbcEnumerator;
import com.yuanfudao.multi.jdbc.connector.table.MultiJdbcPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.test.util.AbstractTestBase;

import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcOptions;
import com.yuanfudao.multi.jdbc.connector.options.MultiJdbcReadOptions;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;

/** multi jdbc source enumerator logical test case. */
public class MultiSourceEnumeratorTests extends AbstractTestBase {

    public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
    public static final String DB_URL_1 = "jdbc:derby:memory:test1";
    public static final String DB_URL_2 = "jdbc:derby:memory:test2";
    public static final String INPUT_TABLE_1 = "jdbcSource_1";
    public static final String INPUT_TABLE_2 = "jdbcSource_2";
    public static final String INPUT_TABLE_3 = "other_3";

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        System.setProperty(
                "derby.stream.error.field", MultiSourceEnumeratorTests.class.getCanonicalName() + ".DEV_NULL");
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
    }

    @Test
    public void testBatchSize() {
        final TestingSplitEnumeratorContext<MultiJdbcPartitionSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        MultiJdbcOptions jdbcOptions =
                MultiJdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:test1;jdbc:derby:memory:test2")
                        .setDriverName(DRIVER_CLASS)
                        .setTableName("JDBCSOURCE.*")
                        .setSchemaName("APP")
                        .build();

        MultiJdbcReadOptions jdbcReadOptions =
                MultiJdbcReadOptions.builder().setPartitionColumnName("id").setBatchSize(2).build();

        MultiJdbcEnumerator multiJdbcEnumerator =
                new MultiJdbcEnumerator(jdbcOptions, jdbcReadOptions, genSchema(), context);
        multiJdbcEnumerator.start();
        Deque<MultiJdbcPartitionSplit> deque = multiJdbcEnumerator.getSplitsQueue();
        Deque<MultiJdbcPartitionSplit> except = new ArrayDeque<>();
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 1 AND 2",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 3 AND 4",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 4 AND 5",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 6 AND 7",
                        DB_URL_1,
                        null,
                        null));

        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 1 AND 2",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 3 AND 4",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 4 AND 5",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 6 AND 7",
                        DB_URL_2,
                        null,
                        null));
        Assert.assertEquals(except.size(), deque.size());
        while (!except.isEmpty()) {
            Assert.assertEquals(except.removeLast(), deque.removeLast());
        }
    }

    @Test
    public void testPartitionNum() {
        final TestingSplitEnumeratorContext<MultiJdbcPartitionSplit> context =
                new TestingSplitEnumeratorContext<>(4);
        MultiJdbcOptions jdbcOptions =
                MultiJdbcOptions.builder()
                        .setDBUrl("jdbc:derby:memory:test1;jdbc:derby:memory:test2")
                        .setDriverName(DRIVER_CLASS)
                        .setTableName("JDBCSOURCE.*")
                        .setSchemaName("APP")
                        .build();

        MultiJdbcReadOptions jdbcReadOptions =
                MultiJdbcReadOptions.builder()
                        .setPartitionColumnName("id")
                        .setNumPartitions(2)
                        .build();

        MultiJdbcEnumerator multiJdbcEnumerator =
                new MultiJdbcEnumerator(jdbcOptions, jdbcReadOptions, genSchema(), context);
        multiJdbcEnumerator.start();
        Deque<MultiJdbcPartitionSplit> deque = multiJdbcEnumerator.getSplitsQueue();
        Deque<MultiJdbcPartitionSplit> except = new ArrayDeque<>();
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 1 AND 2",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 3 AND 4",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 4 AND 5",
                        DB_URL_1,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 6 AND 7",
                        DB_URL_1,
                        null,
                        null));

        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 1 AND 2",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_1 WHERE id BETWEEN 3 AND 4",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 4 AND 5",
                        DB_URL_2,
                        null,
                        null));
        except.addLast(
                new MultiJdbcPartitionSplit(
                        "SELECT id, context FROM APP.JDBCSOURCE_2 WHERE id BETWEEN 6 AND 7",
                        DB_URL_2,
                        null,
                        null));
        Assert.assertEquals(except.size(), deque.size());
        while (!except.isEmpty()) {
            Assert.assertEquals(except.removeLast(), deque.removeLast());
        }
    }

    private String[] genSchema() {
        return ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("context", DataTypes.STRING()))
                .getColumnNames()
                .toArray(new String[0]);
    }
}
