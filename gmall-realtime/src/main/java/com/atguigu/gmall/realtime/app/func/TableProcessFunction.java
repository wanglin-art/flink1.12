package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.MySQLUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * package name is com.atguigu.gmall.realtime.app.func Created by a on 2021/8/7 下午2:36
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

public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(
        OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }


    private Map<String,TableProcess> tableProcessMap = new HashMap<>();

    private Set<String> existsTable = new HashSet<>();
    Connection conn = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        initTableProcessMap();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initTableProcessMap();
            }
        },5000,5000);

    }

    private void initTableProcessMap() {
        System.out.println("更新配置的处理信息");
        List<TableProcess> tableProcess_list = MySQLUtil
            .queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : tableProcess_list) {
            String operateType = tableProcess.getOperateType();
            String sinkTable = tableProcess.getSinkTable();
            String sinkType = tableProcess.getSinkType();
            String sourceTable = tableProcess.getSourceTable();

            //拼接字段创建主键
            String key = sourceTable +":"+operateType;
            //将数据存入结果集合
            tableProcessMap.put(key,tableProcess);
            if ("insert".equals(operateType) && "hbase".equals(sinkType)){
                boolean notExist  = existsTable.add(sinkTable);
                //如果表不在内存中,则在Phoenix中建表
                if (notExist){
                    checkTable(sinkTable, tableProcess.getSinkColumns(), tableProcess.getSinkPK(), tableProcess.getSinkExtend());
                }
            }
            if (tableProcessMap==null || tableProcessMap.isEmpty()){
                throw new RuntimeException("缺少处理信息");
            }
        }
    }

    private void checkTable(String tableName, String sinkColumns, String sinkPK, String sinkExtend) {
        if (sinkPK==null){
            sinkPK = "id";
        }
        if (sinkExtend == null){
            sinkExtend = "";
        }
        //创建字符串拼接对象
        StringBuilder sb = new StringBuilder("create table if not exists "+ GmallConfig.HBASE_SCHEMA + "."+tableName +"(");

        //将列做切分,并拼接至建表语句SQL中
        String[] fieldsArr = sinkColumns.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
           String field =  fieldsArr[i];
            if (sinkPK.equals(field)){
                sb.append(field).append(" varchar primary key ");
            }else{
                sb.append("info.").append(field).append(" varchar");
            }
            if (i<fieldsArr.length -1 ){
                sb.append(",");
            }
        }
        sb.append(" )");
        sb.append(sinkExtend);

        System.out.println("执行语句为:"+sb);
        try {
            PreparedStatement ps = conn.prepareStatement(sb.toString());
            ps.execute();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败!");
        }


    }

    @Override
    public void processElement(JSONObject jsonObject, Context context,
        Collector<JSONObject> collector) throws Exception {

        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        JSONObject data = jsonObject.getJSONObject("data");

        //如果是使用MaxWell的初始化功能,那么type类型为bootstrap-insert ,我们这里也标记为insert ,方便后期处理
        if (type.equals("bootstrap-insert")){
            type = "insert";
            jsonObject.put("type",type);
        }
        //获取配置表的信息
        if (!tableProcessMap.isEmpty()){
            String key = table + ':'+type;
            TableProcess tableProcess = tableProcessMap.get(key);
            if (tableProcess!=null){
                jsonObject.put("sink_table",tableProcess.getSinkTable());
                if (tableProcess.getSinkColumns()!=null && tableProcess.getSinkColumns().length()>0){
                    filterColumn(jsonObject.getJSONObject("data"),tableProcess.getSinkColumns());
                }
            }else{
                System.out.println("no this key:"+key);
            }

            if (tableProcess !=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                context.output(outputTag,jsonObject);
            }else if (tableProcess !=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                collector.collect(jsonObject);
            }
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] split = StringUtils.split(sinkColumns,",");
        Set<Entry<String, Object>> entries = data.entrySet();
        List<String> list = Arrays.asList(split);
        Iterator<Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Entry<String, Object> next = iterator.next();
            if (!list.contains(next.getKey())){
                iterator.remove();
            }
        }

    }
}
