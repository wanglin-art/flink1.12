package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * package name is com.atguigu.gmall.realtime.app.func Created by a on 2021/8/8 下午3:24
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

public class DimSink extends RichSinkFunction<JSONObject> {

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String sink_table = value.getString("sink_table");
        JSONObject data = value.getJSONObject("data");
        if (data != null && data.size() > 0) {
            try{
                String upsertSql = genUpsertSql(sink_table.toUpperCase(), data);
                System.out.println("运行SQL为:"+upsertSql);
                PreparedStatement ps = connection.prepareStatement(upsertSql);
                ps.executeUpdate();
                connection.commit();
                ps.close();
            }catch (Exception e){
                e.printStackTrace();
                throw new RuntimeException("执行sql失败！");
            }
        }
    }

    private String genUpsertSql(String tableName, JSONObject data) {
        Set<String> fields = data.keySet();
        String upsertSql =
            "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + " (" + StringUtils
                .join(fields, ",") + " ) ";
        String valueSql = "values ('" + StringUtils.join(data.values(), "','") + "')";
        return upsertSql + valueSql;
    }
}
