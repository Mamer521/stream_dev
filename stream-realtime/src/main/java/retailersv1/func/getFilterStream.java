package retailersv1.func;

import com.alibaba.fastjson.JSONObject;

import com.common.utils.ConfigUtils;
import com.common.utils.HbaseUtils;
import com.common.utils.JdbcUtils;
import com.common.utils.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import retailersv1.domain.TableProcessDim;

import java.sql.Connection;
import java.util.*;

/**
 * @Package com.retailersv1.func.ProcessSpiltStreamToHBaseDim
 * @Author zhou.han
 * @Date 2024/12/26 10:54
 * @description:
 */
public class getFilterStream extends BroadcastProcessFunction<JSONObject,JSONObject,JSONObject> {

    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDim> configMap =  new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from gmall2024_config.table_process_dwd";
        List<TableProcessDim> tableProcessDims = JdbcUtils.queryList(connection, querySQL, TableProcessDim.class, true);
        // configMap:spu_info -> TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=null)
        for (TableProcessDim tableProcessDim : tableProcessDims ){
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        connection.close();
    }

    public getFilterStream(MapStateDescriptor<String, JSONObject> mapStateDescriptor) {
        this.mapStateDescriptor=mapStateDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //{"op":"c","after":{"is_ordered":0,"cart_price":"Ceqs","sku_num":1,"create_time":1734821068000,"user_id":"1150","sku_id":3,"sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":20224},"source":{"thread":123678,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000002","connector":"mysql","pos":383682973,"name":"mysql_binlog_source","row":0,"ts_ms":1734741512000,"snapshot":"false","db":"realtime_v1","table":"cart_info"},"ts_ms":1734741512593}
//        System.err.println("processElement process -> "+jsonObject.toString());
        //org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator$ReadOnlyContextImpl@52b20462
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("source").getString("table");
        JSONObject broadData = broadcastState.get(tableName);
        // 这里可能为null NullPointerException
        if (broadData != null || configMap.get(tableName) != null){
            //筛选出可以关联的数据
            if (configMap.get(tableName).getSourceTable().equals(tableName)){
//                System.err.println(jsonObject);
                if(!"d".equals(jsonObject.getString("op"))){
                    //获取关联后的发送主题名称
                    String sinkTable = configMap.get(tableName).getSinkTable();

                    JSONObject kafkaData = new JSONObject();
                    kafkaData.put("tableName",tableName);//将主流的表明添加进去
                    kafkaData.put("operation",jsonObject.getString("op"));
                    kafkaData.put("data",jsonObject.getJSONObject("after"));
//                    System.out.println(">>>>>>>"+kafkaData);
//                    collector.collect(kafkaData);

                    ArrayList<JSONObject> list = new ArrayList<>();
                    list.add(kafkaData);

//                    for (JSONObject object : list) {
//                        System.out.println(sinkTable+":"+object.toString());
//                    }

                    KafkaUtils.sinkJson2KafkaMessage(sinkTable,list);
                }
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // {"op":"r","after":{"sink_row_key":"id","sink_family":"info","sink_table":"dim_base_category2","source_table":"base_category2","sink_columns":"id,name,category1_id"}}
//        System.err.println("processBroadcastElement jsonObject -> "+ jsonObject.toString());
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStageDesc', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@39529185, valueSerializer=org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer@59b0797e, assignmentMode=BROADCAST}, backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@4ab01899}
        String op = jsonObject.getString("op");
        if (jsonObject.containsKey("after")){
            String sourceTableName = jsonObject.getJSONObject("after").getString("source_table");
            if ("d".equals(op)){
                broadcastState.remove(sourceTableName);
            }else {
                broadcastState.put(sourceTableName,jsonObject);
//                configMap.put(sourceTableName,jsonObject.toJavaObject(TableProcessDim.class));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}