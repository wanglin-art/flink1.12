package com.atguigu.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * package name is com.atguigu.gmall.realtime.app Created by a on 2021/8/6 下午11:26
 *
 * @author a
 */
//                   _ooOoo_
//                  o8888888o
//                  88" . "88
//                  (| -_- |)
//                  O\  =  /O
//               ____/`---'\____
//             .'  \\|     |//  `.
//            /  \\|||  :  |||//  \
//           /  _||||| -:- |||||-  \
//           |   | \\\  -  /// |   |
//           | \_|  ''\---/''  |   |
//           \  .-\__  `-`  ___/-. /
//         ___`. .'  /--.--\  `. . __
//      ."" '&lt;  `.___\_&lt;|>_/___.'  >'"".
//     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
//     \  \ `-.   \_ __\ /__ _/   .-` /  /
//======`-.____`-.___\_____/___.-`____.-'======
//                   `=---='
//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//           佛祖保佑       永无BUG

public class BaseDBApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME","atguigu");
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDstream = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonDStream = jsonStrDstream.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDStream.filter(
            jsonObject -> {
                boolean flag = jsonObject.getString("table") != null
                    && jsonObject.getJSONObject("data") != null
                    && jsonObject.getString("data").length() > 3;
                return flag;
            }
        );
        filterDS.print(">>>>>>>json");
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterDS.process(
            new TableProcessFunction(hbaseTag)
        );
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print(">>>>>HBASE写入");
        hbaseDS.addSink(new DimSink());
        env.execute();

    }

}
