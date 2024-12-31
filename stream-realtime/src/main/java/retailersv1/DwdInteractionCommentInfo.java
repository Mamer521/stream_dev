package retailersv1;

import com.common.utils.ConfigUtils;
import com.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * mashuai
 * 2024/12/28 11:01
 */

public class DwdInteractionCommentInfo {

    private static final String topicName= "topic_db";
    private static final String hbase_zookeeper= "cdh01:9092,cdh02:9092,cdh03:9092";

    private static final String dwd_interaction_comment_info="dwd_interaction_comment_info";


    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "op string," +
                "db string," +
                "before map<String,String>," +
                "after map<String,String>," +
                "source map<String,String>," +
                "ts_ms bigint," +
                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + hbase_zookeeper + "',\n" +
                "  'properties.group.id' = 'test01',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        Table comment = getComment(tableEnv);

//        comment.execute().print();

        tableEnv.createTemporaryView("comment_info",comment);

        //4.读码表
        createBaseDic(tableEnv);

        //关联数据
        Table table = getTable(tableEnv);
        //写入kafka
        extracted(tableEnv);

        table.execute().print();

//        table.insertInto(dwd_interaction_comment_info).execute();


        env.execute();
    }

    private static void extracted(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table "+dwd_interaction_comment_info+"(\n" +
                "        id STRING,\n" +
                "        user_id STRING,\n" +
                "        nick_name STRING,\n" +
                "        sku_id STRING,\n" +
                "        spu_id STRING,\n" +
                "        order_id STRING,\n" +
                "        appraise_code STRING,\n" +
                "        appraise_name STRING,\n" +
                "        comment_txt STRING,\n" +
                "        create_time STRING)");
        /*
        create table "+dwd_interaction_comment_info+"(
        id STRING,
        user_id STRING,
        nick_name STRING,
        sku_id STRING,
        spu_id STRING,
        order_id STRING,
        appraise_code STRING,
        appraise_name STRING,
        comment_txt STRING,
        create_time STRING)
         */
    }

    private static Table getTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "        id,\n" +
                "        user_id,\n" +
                "        nick_name,\n" +
                "        sku_id,\n" +
                "        spu_id,\n" +
                "        order_id,\n" +
                "        appraise,\n" +
                "        info.dic_name,\n" +
                "        comment_txt,\n" +
                "        create_time\n" +
                "        FROM comment_info AS c\n" +
                "        JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS b\n" +
                "         ON c.appraise = b.rowkey");
    }

    private static void createBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                "                rowkey STRING,\n" +
                "                info ROW<dic_name STRING>,\n" +
                "                PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                "                ) WITH (\n" +
                "                'connector' = 'hbase-2.2',\n" +
                "                'table-name' = 'gmall:dim_base_dic',\n" +
                "                'zookeeper.quorum' = '"+ hbase_zookeeper +"'\n" +
                "                )");

    }

    private static Table getComment(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "        `data`['id'] id,\n" +
                "        `data`['user_id'] user_id,\n" +
                "        `data`['nick_name'] nick_name,\n" +
                "        `data`['sku_id'] sku_id,\n" +
                "        `data`['spu_id'] spu_id,\n" +
                "        `data`['order_id'] order_id,\n" +
                "        `data`['appraise'] appraise,\n" +
                "        `data`['comment_txt'] comment_txt,\n" +
                "        `data`['create_time'] create_time,\n" +
                "        proc_time \n" +
                "        from topic_db\n" +
                "        where `database` = 'gmall' and `type` = 'insert'\n" +
                "        and `table` = 'comment_info'");
    }
}
