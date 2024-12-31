package retailersv1.Catalog;

import com.common.utils.ConfigUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import stream.utils.HiveCatalogUtils;

/**
 * mashuai
 * 2024/12/30 16:54
 */

public class Catalog_select_parent_code {
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";

    private static final String createHbaseDimBaseDicDDL = "create table hbase_dim_base_dic (" +
            "    rk string," +
            "    info row<dic_name string, parent_code string>," +
            "    primary key (rk) not enforced" +
            ")" +
            "with (" +
            "    'connector' = '"+HBASE_CONNECTION_VERSION+"'," +
            "    'table-name' = '"+HBASE_NAME_SPACE+":dim_base_dic'," +
            "    'zookeeper.quorum' = '"+ZOOKEEPER_SERVER_HOST_LIST+"'" +
            ")";
    public static void main(String[] args) {

//        System.setProperty(ConfigUtils.getString("hbase.namespace"),"root");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "E:\\Study\\workspace\\2203A\\stream_dev\\stream-realtime\\src\\main\\resources");
//
//        tEnv.registerCatalog("hive-catalog",hiveCatalog);
//        tEnv.useCatalog("hive-catalog");
//
//        tEnv.executeSql("select rk," +
//                "info.dic_name as dic_name," +
//                "info.parent_code as parent_code " +
//                "from hbase_dim_base_dic").print();

        System.setProperty("HADOOP_USER_NAME","root");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = HiveCatalogUtils.getHiveCatalog("hive-catalog");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql("show tables;").print();
        tenv.executeSql(DROP_TABEL_PREFIX + getCreateTableDDLTableName(createHbaseDimBaseDicDDL));
        tenv.executeSql("show tables;").print();
        tenv.executeSql(createHbaseDimBaseDicDDL).print();
        tenv.executeSql("show tables;").print();
        tenv.executeSql("select * from hbase_dim_base_dic").print();
    }
    public static String getCreateTableDDLTableName(String createDDL){
        return createDDL.split(" ")[2].trim();
    }

}
