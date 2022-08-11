package org.example.olap_web.utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * 连接presto数据源类
 */

public class PrestoConn {
    public static Connection getConnection(){
        Connection conn = null;
        Properties properties = new Properties();
        properties.setProperty("user","root");
        String url = "jdbc:presto://hadoop001:8660/hive/data_warehouse";
        properties.setProperty("SSL","false");
        try {
            conn = DriverManager.getConnection(url, properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
