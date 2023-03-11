package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.twitter.chill.java.UnmodifiableMapSerializer;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.junit.Ignore;

/** description. */
public class OldTest {

    @org.junit.Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testSqlServerExampleSource() throws Exception {
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname("10.207.228.43")
                        .port(1433)
                        .databaseList("ss_2012_lq")
                        .tableList("dbo.full_types")
                        .username("sa")
                        .password("bdp_Test_!234")
                        .startupOptions(StartupOptions.latest())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .serverTimeZone("Asia/Shanghai")
                        .splitSize(2)
                        .build();

        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.num-retained", "30");
        conf.setString("rest.port", "8888");
        conf.setString(
                "state.checkpoints.dir",
                "file:\\" + System.getProperty("user.dir") + "\\checkpoint-dir");

        // 从某个 checkpoint 启动
        //        conf.setString(
        //                "execution.savepoint.path",
        //                "file:\\"
        //                        + System.getProperty("user.dir")
        //                        + "\\checkpoint-dir\\88a0ed4d4ece27fd78aa69811fda5084\\chk-8");

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
