package retailersv1;

import com.alibaba.fastjson.JSONObject;
import com.common.utils.ConfigUtils;
import com.common.utils.EnvironmentSettingUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import retailersv1.func.getFilterStream;
import stream.utils.CdcSourceUtils;


/**
 * mashuai
 * 2024/12/25 13:33
 */

public class DbusCdc2KafkaTopic_db {


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

        connect.process(new getFilterStream(mapStateDescriptor));


        env.execute();

    }

}
