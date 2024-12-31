package retailersv1.Catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * mashuai
 * 2024/12/30 11:03
 */

public class Catalog_createHiveCatalogDDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String createHiveCatalogDDL = "create catalog hive_catalog with (" +
                " 'type'='hive'," +
                " 'default-database'='default'," +
                " 'hive-conf-dir'='E:/Study/workspace/2203A/stream_dev/stream-realtime/src/main/resources')";

        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "E:/Study/workspace/2203A/stream_dev/stream-realtime/src/main/resources");

        tEnv.registerCatalog("hive-catalog",hiveCatalog);

        tEnv.useCatalog("hive-catalog");

        tEnv.executeSql(createHiveCatalogDDL).print();

    }
}
