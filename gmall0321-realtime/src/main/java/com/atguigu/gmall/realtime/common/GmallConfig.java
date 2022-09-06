package com.atguigu.gmall.realtime.common;

/**
 * @ClassName: GmallConfig
 * @Description: TODO 电商实时数仓系统 常量配置类
 * @Author: fanfan
 * @DateTime: 2022年09月05日 18时21分
 * @Version: v1.0
 *
 */
public class GmallConfig {
    public static final String PHOENIX_SCHEMA = "GMALL0321_SCHEMA";
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
