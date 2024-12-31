package stream.utils;

import com.common.utils.ConfigUtils;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * mashuai
 * 2024/12/30 20:12
 */

public class HiveCatalogUtils {

    public static HiveCatalog getHiveCatalog(String catalogName){
        System.setProperty("HADOOP_USER_NAME","root");
        return new HiveCatalog(catalogName, "default", "E:/Study/workspace/2203A/stream_dev/stream-realtime/src/main/resources");
    }
}
