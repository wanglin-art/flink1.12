package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * package name is com.atguigu.gmall.realtime.bean Created by a on 2021/8/7 下午1:41
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

    @Data
public class TableProcess {
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    //来源表
    String sourceTable;

    //操作类型 insert,update,delete
    String operateType;

    //输出类型 hbase kafka
    String sinkType;

    //输出表
    String sinkTable;

    //输出字段
    String  sinkColumns;

    //主键字段
    String sinkPK;

    //建表扩展
    String sinkExtend;



}
