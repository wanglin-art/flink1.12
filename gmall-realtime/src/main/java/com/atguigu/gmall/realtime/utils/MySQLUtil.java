package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.beanutils.BeanUtils;

/**
 * package name is com.atguigu.gmall.realtime.utils Created by a on 2021/8/7 下午1:54
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

public class MySQLUtil {

    public static <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //注册驱动
            Class.forName("com.mysql.jdbc.Driver");

            //建立连接
            conn = DriverManager.getConnection(
                "jdbc:mysql://hadoop102:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false",
                "root", "qwer1234");
            //创建数据库操作对象
            ps = conn.prepareStatement(sql);

            //执行SQL语句
            rs = ps.executeQuery();

            //处理结果集
            ResultSetMetaData md = rs.getMetaData();

            //声明集合对象,用于封装返回结果
            List<T> resultList = new ArrayList<>();

            //每循环一次,获取一次查询结果
            while (rs.next()) {
                T obj = clazz.newInstance();
                //对查询到的列进行遍历
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String propertyName = md.getColumnName(i);
                    if (underScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, propertyName);
                    }
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询mysql失败！");
        } finally {
            //释放资源
            if (rs!=null){
                try {
                    rs.close();
                } catch (SQLException s) {
                    s.printStackTrace();
                }
            }

            if (ps!=null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (conn!=null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process",
            TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }

}
