package com.atguigu.gmall.realtime.util;

/**
 * @ClassName: MysqlUtil
 * @Description: TODo 操作mysql的工具类，从mysql数据库中的字典表读取为flink lookup表，以便维度退化
 * @Author: fanfan
 * @DateTime: 2022年09月08日 18时48分
 * @Version: v1.0
 */
public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`(" +
                "`dic_code` string," +
                "`dic_name` string," +
                "`parent_code` string," +
                "`create_time` timestamp," +
                "`operate_time` timestamp," +
                "primary key(`dic_code`) not enforced" +
                ")" + mysqlLookUpTableExt("base_dic");
    }

    public static String mysqlLookUpTableExt(String tableName) {
        // 建表扩展语句
        String DDLExt = "WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall0321'," +
                "'table-name' = '" + tableName + "'," +
                "'lookup.cache.max-rows' = '500'," +
                "'lookup.cache.ttl' = '1 hour'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'" +
                ")";
        return DDLExt;
    }
}
