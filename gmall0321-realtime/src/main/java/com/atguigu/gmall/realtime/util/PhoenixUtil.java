package com.atguigu.gmall.realtime.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName: PhoenixUtil
 * @Description: TODO 操作Phoenix的工具类
 *                  通过德鲁伊连接池，创建连接供phoenix来使用
 * @Author: fanfan
 * @DateTime: 2022年09月06日 00时56分
 * @Version: v1.0
 */
public class PhoenixUtil {
    /**
     * @param sql 建表sql
     * @param conn 数据库连接对象
     */
    public static void executeSql(String sql, Connection conn){
        // 1. 创建数据库操作对象
        PreparedStatement ps = null;
        try {
            // 获取数据库操作对象
            ps = conn.prepareStatement(sql);
            // 2. 执行sql
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // 3. 释放资源
            // TODO 注意: 从数据库连接池中获取连接后，再释放，连接会被连接池回收，不会真的释放
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
