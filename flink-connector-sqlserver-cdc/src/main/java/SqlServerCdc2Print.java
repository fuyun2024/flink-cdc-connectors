import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.twitter.chill.java.UnmodifiableMapSerializer;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecord;
import com.ververica.cdc.connectors.sf.deserialization.HybridSourceRecordDeserialization;
import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

/** Postgres cdc to print. */
public class SqlServerCdc2Print {

    public SqlServerCdc2Print() {}

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //        conf.setString("state.backend", "filesystem");
        //        conf.setString("state.checkpoints.num-retained", "3");
        //        conf.setString(
        //                "state.checkpoints.dir",
        //                "file:\\" + System.getProperty("user.dir") + "\\checkpoint-dir");

        // 从某个 checkpoint 启动
        //        conf.setString(
        //                "execution.savepoint.path",
        //                "file:\\"
        //                        + System.getProperty("user.dir")
        //                        + "\\checkpoint-dir\\d6fc875f3b3f5db2f91f3ee53fe82394\\chk-4");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        Class<?> unmodMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodMap, UnmodifiableMapSerializer.class);
        env.getConfig()
                .addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);

        env.enableCheckpointing(1000 * 20);
        env.setParallelism(1);

        // 全增量并行写 sql server
        SqlServerSourceBuilder.SqlServerIncrementalSource<HybridSourceRecord> sqlSource =
                new SqlServerSourceBuilder<HybridSourceRecord>()
                        .hostname("10.207.228.43")
                        .port(1433)
                        .databaseList("ss_2012_lq")
                        .tableList(".*")
                        .username("sa")
                        .password("bdp_Test_!234")
                        .startupOptions(StartupOptions.latest())
                        .deserializer(new HybridSourceRecordDeserialization())
                        .serverTimeZone("Asia/Shanghai")
                        .build();

        env.fromSource(sqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc").print();
        env.execute("Print SqlServer Snapshot + Change Stream");
    }
}
