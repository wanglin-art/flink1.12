package com.atguigu.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * package name is com.atguigu.gmall.realtime.app Created by a on 2021/8/4 上午11:26
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

public class BaseLogApp {
        private static final String TOPIC_START = "dwd_start_log";
        private static final String TOPIC_PAGE = "dwd_page_log";
        private static final String TOPIC_DISPLAY = "dwd_display_log";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        //从kafka中获取数据
        String groupId = "ods_group";
        String topic = "ods_base_log";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDataStream.map(
            new MapFunction<String, JSONObject>() {
                @Override
                public JSONObject map(String s) throws Exception {
                    return JSON.parseObject(s);
                }
            }
        );
        jsonObjectDS.print("json>>>>>>>>");
        env.execute();
    }
}
