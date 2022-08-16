package com.sean.spark.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class DataSourceUtil implements Serializable {

    private static DataSourceUtil dataSourceUtil = null;
    private static DataSource dataSource = null;

    private DataSourceUtil() {
    }

    static {
        dataSourceUtil = new DataSourceUtil();

        Properties prop = new Properties();
        prop.setProperty(DruidDataSourceFactory.PROP_URL, ConfigurationManager.getProperty("jdbc.url"));
        prop.setProperty(DruidDataSourceFactory.PROP_USERNAME, ConfigurationManager.getProperty("jdbc.user"));
        prop.setProperty(DruidDataSourceFactory.PROP_PASSWORD, ConfigurationManager.getProperty("jdbc.password"));
        //初始化大小
        prop.setProperty(DruidDataSourceFactory.PROP_INITIALSIZE, "5");
        //最大连接
        prop.setProperty(DruidDataSourceFactory.PROP_MAXACTIVE, "20");
        //最小连接
        prop.setProperty(DruidDataSourceFactory.PROP_MINIDLE, "15");
        //等待时长
        prop.setProperty(DruidDataSourceFactory.PROP_MAXWAIT, "60000");
        //配置多久进行一次检测
        prop.setProperty(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS, "20000");
        //配置连接池中最小生存时间 单位毫秒
        prop.setProperty(DruidDataSourceFactory.PROP_MINEVICTABLEIDLETIMEMILLIS, "600000");
        prop.setProperty(DruidDataSourceFactory.PROP_VALIDATIONQUERY, "select 1");
        prop.setProperty(DruidDataSourceFactory.PROP_TESTWHILEIDLE, "true");
        prop.setProperty(DruidDataSourceFactory.PROP_TESTONBORROW, "false");
        prop.setProperty(DruidDataSourceFactory.PROP_TESTONRETURN, "false");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //提供获取连接的方法
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    //提供关闭资源的方法
    public static void closeResource(ResultSet rs, PreparedStatement pstm, Connection conn) {
        //关闭结果集
        closeResultSet(rs);
        //关闭语句执行者
        closePreparedStatement(pstm);
        //关闭连接
        closeConnection(conn);
    }

    //关闭连接
    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭语句执行者
    private static void closePreparedStatement(PreparedStatement pstm) {
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭结果集
    private static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
