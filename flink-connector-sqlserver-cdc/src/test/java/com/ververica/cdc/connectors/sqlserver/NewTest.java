package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.twitter.chill.java.UnmodifiableMapSerializer;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord;
import com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecordDeserialization;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.junit.Ignore;

/** description. */
public class NewTest {

    @org.junit.Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testSqlServerExampleSource() throws Exception {
        SqlServerSourceBuilder.SqlServerIncrementalSource<HybridSourceRecord> sqlServerSource =
                new SqlServerSourceBuilder<HybridSourceRecord>()
                        .hostname("10.207.228.43")
                        .port(1433)
                        .databaseList("ss_2012_lq")
                        .tableList(".*")
                        .username("sa")
                        .password("bdp_Test_!234")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new HybridSourceRecordDeserialization())
                        .serverTimeZone("Asia/Shanghai")
                        .splitSize(2)
                        .parallelReadEnabled(true)
                        .includeSchemaChanges(false)
                        .getTableChangeUrl("http://10.206.228.20:5000/get_data")
                        .reportReachBinlogUrl("http://10.206.228.20:5000/tmp_write_data")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.num-retained", "30");
        conf.setString("rest.port", "8888");
        conf.setString(
                "state.checkpoints.dir",
                "file:\\" + System.getProperty("user.dir") + "\\checkpoint-dir");

        // 从某个 checkpoint 启动
        conf.setString(
                "execution.savepoint.path",
                "file:\\"
                        + System.getProperty("user.dir")
                        + "\\checkpoint-dir\\aa93e3816e31ca70f6bb536ee26f2a15\\chk-30");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        Class<?> unmodMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodMap, UnmodifiableMapSerializer.class);
        env.getConfig()
                .addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);

        // enable checkpoint
        env.enableCheckpointing(10000);
        // set the source parallelism to 2
        env.fromSource(
                        sqlServerSource,
                        WatermarkStrategy.noWatermarks(),
                        "SqlServerIncrementalSource")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print SqlServer Snapshot + Change Stream");
    }
}
