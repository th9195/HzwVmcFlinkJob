package com.hzw.fdc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class LocalPropertiesConfig {
    private static final Logger LOG = LoggerFactory.getLogger(LocalPropertiesConfig.class);
    public final static Properties config = new Properties();

    static {
        InputStream profile = LocalPropertiesConfig.class.getClassLoader().getResourceAsStream("application.properties");
        try {
            config.load(profile);
        } catch (Exception e) {
            LOG.info("load profile error!");
        }
        for(Map.Entry<Object,Object> kv:config.entrySet()){
            LOG.info(kv.getKey()+"="+kv.getValue());
            System.out.println(kv.getKey()+"="+kv.getValue());
        }
    }

    public static void main(String[] args) {
        String config_path = config.getProperty("config_path");
        System.out.println(config_path);
    }
}