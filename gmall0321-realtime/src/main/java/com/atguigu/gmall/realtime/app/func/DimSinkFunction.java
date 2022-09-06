package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 * @ClassName: DimSinkFunction
 * @Description: TODO 将维度流中的数据 sink到 phoenix表中
 * @Author: fanfan
 * @DateTime: 2022年09月06日 03时25分
 * @Version: v1.0
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 数据库连接池对象
    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    /**
     * 每来一条数据调用一次
     * TODO  --> 将流中的json写到phoenix对应的表中
     * upsert into 表空间.表 (a,b,c) values(aa,bb,cc)
     *
     * @param jsonObj
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {

        String tableName = jsonObj.getString("sink_table");
        // 清洗jsonObj，过滤掉多于的 sink_table字段
        //  {"tm_name":"banzhang","sink_table":"dim_base_trademark","id":12}
        //  {"tm_name":"banzhang","id":12}
        jsonObj.remove("sink_table");

        // 拼接phoenix中建表语句:
        String upsertSql = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + tableName
                + " (" + StringUtils.join(jsonObj.keySet(), ",") + ")" +
                " values('" + StringUtils.join(jsonObj.values(), "','") + "')";

        System.out.println("向phoenix表中插入的数据的SQL: " + upsertSql);

        // 执行SQL
        Connection conn = druidDataSource.getConnection();
        PhoenixUtil.executeSql(upsertSql, conn);
    }
}
