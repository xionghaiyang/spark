package com.sean.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Properties;

public class ConfigurationManager {

    private static ConfigurationManager configurationManager = null;
    private static Properties prop = new Properties();

    private ConfigurationManager() {
    }

    static {
        configurationManager = new ConfigurationManager();
        InputStream in = ConfigurationManager.class
                .getClassLoader()
                .getResourceAsStream("application.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //获取配置项
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static boolean getBoolean(String key) {
        return Boolean.valueOf(prop.getProperty(key));
    }

    public static Properties loadProperties(String v_path) {
        Configuration conf = new Configuration();
        Properties prop = new Properties();
        Path path = new Path(v_path);
        FileSystem fs = null;
        FSDataInputStream in = null;
        try {
            fs = FileSystem.get(conf);
            in = fs.open(path);
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            IOUtils.closeStream(in);
        }
        return prop;
    }

    public static Properties localProperties(String v_path) {
        Properties prop = new Properties();
        File file = new File(v_path);
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            prop.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
        return prop;
    }

}
