/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.table;

import com.ververica.cdc.connectors.oracle.utils.OracleTestUtils;
import com.ververica.cdc.connectors.oracle.utils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.assertEqualsInAnyOrder;
import static com.ververica.cdc.connectors.oracle.source.OracleSourceTestBase.assertEqualsInOrder;

/** Integration tests to check oracle-cdc works well under different Oracle server timezone. */
@RunWith(Parameterized.class)
public class OracleTimezoneITCase {

    private static final Logger LOG = LoggerFactory.getLogger(OracleTimezoneITCase.class);
    private static TemporaryFolder tempFolder;
    private static File resourceFolder;
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Parameterized.Parameter public Boolean incrementalSnapshot;

    @Parameterized.Parameters(name = "incrementalSnapshot: {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @Before
    public void setup() throws Exception {
        resourceFolder =
                Paths.get(
                                Objects.requireNonNull(
                                                OracleTimezoneITCase.class
                                                        .getClassLoader()
                                                        .getResource("."))
                                        .toURI())
                        .toFile();
        tempFolder = new TemporaryFolder(resourceFolder);
        tempFolder.create();
        if (incrementalSnapshot) {
            env.setParallelism(4);
            env.enableCheckpointing(200);
        } else {
            env.setParallelism(1);
        }
    }

    @Test
    public void testMySqlServerInBerlin() throws Exception {
        testTemporalTypesWithMySqlServerTimezone("Europe/Berlin");
    }

    @Test
    public void testMySqlServerInShanghai() throws Exception {
        testTemporalTypesWithMySqlServerTimezone("Asia/Shanghai");
    }

    private void testTemporalTypesWithMySqlServerTimezone(String timezone) throws Exception {
        OracleContainer oracleContainer = OracleTestUtils.ORACLE_CONTAINER.withLogConsumer(new Slf4jLogConsumer(LOG));


        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(oracleContainer)).join();
        LOG.info("Containers are started.");

        UniqueDatabase fullTypesDatabase =
                new UniqueDatabase(oracleContainer, "XE","column_type_test",
                        oracleContainer.getUsername(), oracleContainer.getPassword());
        fullTypesDatabase.createAndInitialize();

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    `ID` INT NOT NULL,\n"
                                + "    VAL_DATE TIMESTAMP,\n"
                                + "    VAL_TS TIMESTAMP,\n"
                                + "    VAL_TS_PRECISION2 TIMESTAMP(2),\n"
                                + "    VAL_TS_PRECISION4 TIMESTAMP(4),\n"
                                + "    VAL_TS_PRECISION9 TIMESTAMP(9),\n"
                                + "    VAL_TSTZ STRING,\n"
                                + "    VAL_TSLTZ TIMESTAMP WITH LOCAL TIME ZONE,\n"
                                + "    VAL_INT_YTM BIGINT,\n"
                                + "    VAL_INT_DTS BIGINT,\n"
                                + "    primary key (`ID`) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'oracle-cdc',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'schema-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '%s',"
                                + " 'server-time-zone'='%s'"
                                + ")",
                        oracleContainer.getHost(),
                        oracleContainer.getOraclePort(),
                        fullTypesDatabase.getUsername(),
                        fullTypesDatabase.getPassword(),
                        fullTypesDatabase.getDatabaseName(),
                        "DEBEZIUM",
                        "full_types",
                        incrementalSnapshot,
                        getSplitSize(),
                        timezone);
        tEnv.executeSql(sourceDDL);

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "select VAL_DATE, VAL_TS, VAL_TS_PRECISION2, VAL_TS_PRECISION4, VAL_TS_PRECISION9 from full_types");

        CloseableIterator<Row> iterator = result.collect();
        String[] expectedSnapshot =
                new String[] {
                    "+I[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22]"
                };
        assertEqualsInAnyOrder(
                Arrays.asList(expectedSnapshot), fetchRows(iterator, expectedSnapshot.length));

        try (Connection connection = fullTypesDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {

            statement.execute(
                    "UPDATE full_types SET timestamp_c = '2020-07-17 18:33:22' WHERE id=1;");
        }

        String[] expectedBinlog =
                new String[] {
                    "-U[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:00:22]",
                    "+U[2020-07-17, 18:00:22, 2020-07-17T18:00:22.123, 2020-07-17T18:00:22.123456, 2020-07-17T18:33:22]"
                };

        assertEqualsInOrder(
                Arrays.asList(expectedBinlog), fetchRows(iterator, expectedBinlog.length));

        result.getJobClient().get().cancel().get();
    }

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        if (incrementalSnapshot) {
            return serverId + "-" + (serverId + env.getParallelism());
        }
        return String.valueOf(serverId);
    }

    private int getSplitSize() {
        if (incrementalSnapshot) {
            // test parallel read
            return 4;
        }
        return 0;
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String buildMySqlConfigWithTimezone(String timezone) {
        try {
            File folder = tempFolder.newFolder(String.valueOf(UUID.randomUUID()));
            Path cnf = Files.createFile(Paths.get(folder.getPath(), "my.cnf"));
            String mysqldConf =
                    "[mysqld]\n"
                            + "binlog_format = row\n"
                            + "log_bin = mysql-bin\n"
                            + "server-id = 223344\n"
                            + "binlog_row_image = FULL\n";
            String timezoneConf = "default-time_zone = '" + timezone + "'\n";
            Files.write(
                    cnf,
                    Collections.singleton(mysqldConf + timezoneConf),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.APPEND);
            return Paths.get(resourceFolder.getAbsolutePath()).relativize(cnf).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create my.cnf file.", e);
        }
    }
}
