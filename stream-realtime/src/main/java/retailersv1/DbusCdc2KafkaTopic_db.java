package retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.common.utils.ConfigUtils;
import com.common.utils.EnvironmentSettingUtils;
import com.common.utils.KafkaUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import stream.utils.CdcSourceUtils;

import java.util.HashMap;

/**
 * mashuai
 * 2024/12/25 13:33
 */

public class DbusCdc2KafkaTopic_db {

    private static HashMap<String,String> kafkatable = new HashMap<>();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        MySqlSource<String> mySQLCdcSource_gmall = CdcSourceUtils.getMySQLCdcSource(ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial());

        MySqlSource<String> mySQLCdcSource_gmall_config = CdcSourceUtils.getMySQLCdcSource(ConfigUtils.getString("mysql.databases.conf"),
                ConfigUtils.getString("mysql.databases.conf") + ".table_process_dwd",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial());


        DataStreamSource<String> streamSource_gmall = env.fromSource(mySQLCdcSource_gmall, WatermarkStrategy.noWatermarks(), "mysql_gmall");
        DataStreamSource<String> streamSource_gmall_config = env.fromSource(mySQLCdcSource_gmall_config, WatermarkStrategy.noWatermarks(), "gmall_config");

//        streamSource_gmall.print("主流数据:");
//        streamSource_gmall_config.print("配置:");

        SingleOutputStreamOperator<JSONObject> stream_gmall = streamSource_gmall.map(JSONObject::parseObject)
                .uid("streamSource_gmall")
                .name("streamSource_gmall")
                .setParallelism(1);

        SingleOutputStreamOperator<JSONObject> stream_gmall_config = streamSource_gmall_config
                .map(JSONObject::parseObject)
                .uid("streamSource_gmall_config")
                .name("streamSource_gmall_config").setParallelism(1);
//        {"op":"r","after":{"source_type":"update","sink_table":"dwd_tool_coupon_use","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status"},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024_config","table":"table_process_dwd"},"ts_ms":1735129427170}
//        stream_gmall_config.print("配置");
        SingleOutputStreamOperator<JSONObject> stream_gmall_config_end = stream_gmall_config.map(s -> {
            s.remove("source");
            s.remove("transaction");
            JSONObject jsonObject = new JSONObject();
            if ("d".equals(s.getString("op"))) {
                jsonObject.put("before", s.getJSONObject("before"));
            } else {
                jsonObject.put("after", s.getJSONObject("after"));
            }
            jsonObject.put("op", s.getString("op"));
            return jsonObject;
        }).uid("stream_config")
                .name("stream_config");
//        创建广播流
        MapStateDescriptor<String, JSONObject> mapStateDescriptor = new MapStateDescriptor<>("status", String.class, JSONObject.class);
//        将配置表放入广播流中
        BroadcastStream<JSONObject> broadcast = stream_gmall_config_end.broadcast(mapStateDescriptor);
//      用主流数据与广播流做连接
        BroadcastConnectedStream<JSONObject, JSONObject> connect = stream_gmall.connect(broadcast);

        SingleOutputStreamOperator<JSONObject> process = connect.process(new BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext context, Collector<JSONObject> collector) throws Exception {
                ReadOnlyBroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
//                {"op":"r","after":{"log":"{\"common\":{\"ar\":\"24\",\"ba\":\"realme\",\"ch\":\"xiaomi\",\"is_new\":\"1\",\"md\":\"realme Neo2\",\"mid\":\"mid_64\",\"os\":\"Android 13.0\",\"sid\":\"db23d33a-eea8-4d6c-b420-8772c6b5e408\",\"uid\":\"61\",\"vc\":\"v2.1.134\"},\"displays\":[{\"item\":\"16\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":0},{\"item\":\"14\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":1},{\"item\":\"15\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":2},{\"item\":\"16\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":3},{\"item\":\"32\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":4},{\"item\":\"34\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":5},{\"item\":\"12\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":6},{\"item\":\"34\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":7},{\"item\":\"31\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":8},{\"item\":\"26\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":9},{\"item\":\"12\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":10},{\"item\":\"1\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":11},{\"item\":\"26\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":12},{\"item\":\"35\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":13},{\"item\":\"16\",\"item_type\":\"sku_id\",\"pos_id\":5,\"pos_seq\":14}],\"page\":{\"during_time\":11874,\"last_page_id\":\"good_detail\",\"page_id\":\"cart\"},\"ts\":1734844662018}","id":2926},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall","table":"z_log"},"ts_ms":1735134494823}
//                System.out.println("主流:"+jsonObject);
                String tableName = jsonObject.getJSONObject("source").getString("table");
                JSONObject connect_table = broadcastState.get(tableName);
                System.out.println(connect_table);

                if (connect_table!=null){

                }

            }

            @Override
            public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                {"op":"r","after":{"source_type":"update","sink_table":"dwd_tool_coupon_use","source_table":"coupon_use","sink_columns":"id,coupon_id,user_id,order_id,using_time,used_time,coupon_status"}}
//                System.out.println("配置:"+value);
                BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
                if (jsonObject.containsKey("after")) {
                    String source_table = jsonObject.getJSONObject("after").getString("source_table");

                    if ("d".equals(jsonObject.getString("op"))) {
                        broadcastState.remove(source_table);
                    } else {
                        broadcastState.put(source_table, jsonObject);
                    }
                }

            }
        });


        env.execute();

    }

}
