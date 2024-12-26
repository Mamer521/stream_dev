package retailersv1;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.common.utils.ConfigUtils;
import com.common.utils.EnvironmentSettingUtils;
import com.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 *  mashuai
 * 2024/12/24 11:00
 */

public class DbusCdc2KafkaTopic_log {

    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("errTag") {};
    private static final OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("startTag") {};
    private static final OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("displayTag") {};
    private static final OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("actionTag") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        KafkaSource<String> stringKafkaSource = KafkaUtils.
                buildKafkaSource(kafka_bootstrap_servers,
                        kafka_topic_base_log_data,
                        "group1",
                        OffsetsInitializer.earliest());
        DataStreamSource<String> streamSource = env.fromSource(stringKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

//        streamSource.print("数据");
        //1.ETL清洗数据
        SingleOutputStreamOperator<String> etl_process = streamSource.
                filter(x -> JSON.parseObject(x).getJSONObject("common") != null
                        && JSON.parseObject(x).getJSONObject("common").getString("mid") != null
                        && !"".equals(JSON.parseObject(x).getJSONObject("common").getString("mid")));

        SingleOutputStreamOperator<String> etl_line_process = etl_process.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> JSON.parseObject(event).getLong("ts")));
//        etl_process.print("数据清洗");
        //2.新老用户
        SingleOutputStreamOperator<JSONObject> newold_process = etl_line_process
                .keyBy(x->JSON.parseObject(x).getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<String, JSONObject>() {

            ValueState<String> state;

            @Override
            public void open(Configuration parameters) {
                state = getRuntimeContext().getState(new ValueStateDescriptor<>("is_new", String.class));
            }

            @Override
            public JSONObject map(String value) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");

                Long ts = jsonObject.getLong("ts");
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                String format = simpleDateFormat.format(ts);
                String is_new = common.getString("is_new");

                Date date = new Date();
                String format1 = simpleDateFormat.format(date);

                if ("1".equals(is_new)) {
                    if (state.value() != null && !format.equals(format1)) {
                        common.put("is_new", 0);
                    } else {
                        state.update(is_new+":"+format);
                    }
                }
                return jsonObject;
            }
        });



//        OutputTag<JSONObject> start = new OutputTag<JSONObject>("start"){};
//        OutputTag<JSONObject> actions = new OutputTag<JSONObject>("actions"){};
//        OutputTag<JSONObject> displays = new OutputTag<JSONObject>("displays"){};
//        OutputTag<JSONObject> err = new OutputTag<JSONObject>("err"){};
//        OutputTag<JSONObject> page = new OutputTag<JSONObject>("page"){};


        SingleOutputStreamOperator<JSONObject> process = newold_process.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) {

                JSONObject err = jsonObject.getJSONObject("err");
                if (err!=null){
                    context.output(errTag,err);
                }

                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                if (start!=null){
                    context.output(startTag,start);
                    jsonObject.remove("start");

                }else if (page!=null){
                    JSONArray displays1 = jsonObject.getJSONArray("displays");

                    if (displays1!=null){
                        for (int i=0;i<displays1.size();i++){
                            JSONObject display = displays1.getJSONObject(i);
                            display.put("page",page);
                            display.put("common",common);
                            display.put("ts",ts);
                            context.output(displayTag,display);

                        }
                        jsonObject.remove("display");

                    }
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions!=null){
                        for (int i=0;i<actions.size();i++){
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page",page);
                            action.put("common",common);
                            action.put("ts",ts);
                            context.output(actionTag,action);
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject);
                }
            }
        });

        SideOutputDataStream<JSONObject> start_sideOutput = process.getSideOutput(startTag);
        SideOutputDataStream<JSONObject> actions_sideOutput = process.getSideOutput(actionTag);
//        SideOutputDataStream<JSONObject> page_sideOutput = process.getSideOutput(pageTag);
        SideOutputDataStream<JSONObject> displays_sideOutput = process.getSideOutput(displayTag);
        SideOutputDataStream<JSONObject> err_sideOutput = process.getSideOutput(errTag);

        process.print("页面日志>>>>>");
        start_sideOutput.print("开启日志>>>>>");
        actions_sideOutput.print("动作日志>>>>>");
        displays_sideOutput.print("曝光日志>>>>>");
        err_sideOutput.print("错误日志>>>>>");


        //启动
        start_sideOutput.map(x -> x.toJSONString()).sinkTo(
                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_start_log)
        ).uid("sink_to_kafka_start_sideOutput").name("sink_to_kafka_start_sideOutput");

        //动作
        actions_sideOutput.map(x -> x.toJSONString()).sinkTo(
                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_action_log)
        ).uid("sink_to_kafka_actions_sideOutput").name("sink_to_kafka_actions_sideOutput");
        //页面
        process.map(x -> x.toJSONString()).sinkTo(
                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_page_topic)
        ).uid("sink_to_kafka_page_sideOutput").name("sink_to_kafka_page_sideOutput");
        //曝光
        displays_sideOutput.map(x -> x.toJSONString()).sinkTo(
                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_display_log)
        ).uid("sink_to_kafka_displays_sideOutput").name("sink_to_kafka_displays_sideOutput");
        //错误
        err_sideOutput.map(x -> x.toJSONString()).sinkTo(
                KafkaUtils.buildKafkaSink(kafka_bootstrap_servers,kafka_err_log)
        ).uid("sink_to_kafka_err_sideOutput").name("sink_to_kafka_err_sideOutput");


        env.disableOperatorChaining();
        env.execute();
    }
}
