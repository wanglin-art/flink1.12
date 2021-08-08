package com.atguigu.gmall.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
        env.setParallelism(1);

        //从kafka中获取数据
        String groupId = "ooo";
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
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(
            data -> data.getJSONObject("common").getString("mid")
        );

        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedDS.map(
            new RichMapFunction<JSONObject, JSONObject>() {
                //声明第一次访问日期
                private ValueState<String> firstVisitDataState;

                //声明日期数据格式化对象
                private SimpleDateFormat sdf;

                @Override
                public void open(Configuration parameters) throws Exception {
                    System.out.println("本次打开 open 方法");
                    firstVisitDataState = getRuntimeContext().getState(
                        new ValueStateDescriptor<String>("newMidDateState", String.class));
                    sdf = new SimpleDateFormat("yyyyMMdd");
                }

                @Override
                public JSONObject map(JSONObject jsonObject) throws Exception {

                    String is_new = jsonObject.getJSONObject("common").getString("is_new");
                    Long ts = jsonObject.getLong("ts");
                    if ("1".equals(is_new)) {
                        String newMidDate = firstVisitDataState.value();
                        String tsDate = sdf.format(new Date(ts));
                        if (newMidDate != null && newMidDate.length() != 0) {
                            if (!newMidDate.equals(tsDate)) {
                                jsonObject.getJSONObject("common").put("is_new", 0);
                            }
                        } else {
                            firstVisitDataState.update(tsDate);
                        }
                    }
                    return jsonObject;
                }
            }
        );

        OutputTag<String> startTag  = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag  = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = midWithNewFlagDS.process(
            new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject jsonObject, Context context,
                    Collector<String> collector) throws Exception {
                    JSONObject start = jsonObject.getJSONObject("start");
                    String dataStr = jsonObject.toString();
                    if (start != null && start.size() > 0) {
                        context.output(startTag, dataStr);
                    } else {
                        JSONArray displays = jsonObject.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject displayObj = displays.getJSONObject(i);
                                String pageId = jsonObject.getJSONObject("page")
                                    .getString("page_id");
                                displayObj.put("page_id", pageId);
                                context.output(displayTag, displayObj.toString());
                            }
                        } else {
                            collector.collect(dataStr);
                        }
                    }
                }
            }
        );

        DataStream<String> start_sideOutput = pageDS.getSideOutput(startTag);
        DataStream<String> display_sideOutput = pageDS.getSideOutput(displayTag);
        FlinkKafkaProducer<String> start_kafka_prod = MyKafkaUtil.getKafkSink(TOPIC_START);
        FlinkKafkaProducer<String> display_kafka_prod = MyKafkaUtil.getKafkSink(TOPIC_DISPLAY);
        FlinkKafkaProducer<String> page_kafka_prod = MyKafkaUtil.getKafkSink(TOPIC_PAGE);

        start_sideOutput.addSink(start_kafka_prod);
        display_sideOutput.addSink(display_kafka_prod);
        pageDS.addSink(page_kafka_prod);
        env.execute();
    }
}
