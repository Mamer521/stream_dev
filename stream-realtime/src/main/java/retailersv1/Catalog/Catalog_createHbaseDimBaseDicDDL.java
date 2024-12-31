package retailersv1.Catalog;

import com.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * mashuai
 * 2024/12/30 10:40
 */

public class Catalog_createHbaseDimBaseDicDDL {

    private static final String HBASE_NAME_SPACE="stream_dev";
    private static final String HBASE_CONNECTION_VERSION="hbase-2.2";

    private static final String createHbaseDimBaseDicDDL = "create table hbase_dim_base_dic(" +
            "rk string," +
            "info row<dic_name string,parent_code string>," +
            "primary key (rk) not enforced" +
            ")with(" +
            "   'connector' = '"+HBASE_CONNECTION_VERSION+"'," +
            "   'table-name' = '"+HBASE_NAME_SPACE+":dim_base_dic'," +
            "   'zookeeper.quorum' = '"+ ConfigUtils.getString("zookeeper.server.host.list") +"')";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "E:\\Study\\workspace\\2203A\\stream_dev\\stream-realtime\\src\\main\\resources");

        tEnv.registerCatalog("hive-catalog",hiveCatalog);
        tEnv.useCatalog("hive-catalog");
        tEnv.executeSql(createHbaseDimBaseDicDDL);
    }
}
