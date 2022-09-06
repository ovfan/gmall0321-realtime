package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName: TableProcessFunction
 * @Description: TODO 处理维度数据
 * @Author: fanfan
 * @DateTime: 2022年09月02日 17时45分
 * @Version: v1.0
 */

/*
 * @param <IN1> The input type of the non-broadcast side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private DruidDataSource druidDataSource;

    /**
     * TODO TableProcessFunction生命周期开始
     *
     * @param parameters
     * @throws Exception Description: 连接数据库的connection 长时间未使用，会被自动回收，所以需要数据库连接池来获取连接对象
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO 获取德鲁伊 数据库连接池 对象
        druidDataSource = DruidDSUtil.createDataSource();
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // TODO 处理 主流中的数据

    /**
     * 需要从流中筛选出维度数据，并且过滤掉不需要向下游传递的字段，最终收集jsonObj并向下游发送
     *
     * @param jsonObj
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取当前处理业务数据库的表名
        String tableName = jsonObj.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 根据业务数据库表名 从广播状态中获取配置信息
        TableProcess tableProcess = broadcastState.get(tableName);

        // 如果在广播状态中查找到该表，则说明是维度表
        if (tableProcess != null) {
            // TODO 将维度数据中的data内容，整理并向下游发送
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //   1.1 过滤出不需要的字段(建表字段中不包含的字段)
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(dataJsonObj, sinkColumns);

            // TODO 向下游传递的data数据不知道来自于哪张表，所以需要加工一下，把表名放进 dataJsonObj中
            // 获取表名 获取维度数据输出的目的地   {"tm_name":"banzhang","sink_table":"dim_base_trademark","id":12}
            String sinkTableName = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table",sinkTableName);

            out.collect(dataJsonObj);
            System.out.println("维度数据: " + dataJsonObj.toString());
        }



    }

    /**
     * TODO 过滤掉不需要向下游传递的维度表字段
     * dataJsonObj :{"tm_name":"banzhang","logo_url":"aaa","id":12}
     * sinkColumns : id,tm_name
     * @param jsonObject
     * @param sinkColumns
     */
    private void filterColumn(JSONObject jsonObject, String sinkColumns) {
        String[] cloumnsArr = sinkColumns.split(",");
        // 将数组转换为 集合对象操作
        List<String> columnsList = Arrays.asList(cloumnsArr);
        Set<Map.Entry<String, Object>> entrySets = jsonObject.entrySet();

        Iterator<Map.Entry<String, Object>> iterator = entrySets.iterator();
        for (;iterator.hasNext();){
            Map.Entry<String, Object> next = iterator.next();
            if(!columnsList.contains(next.getKey())){
                iterator.remove();
            }
        }
        // 另一种写法: entrySets.removeIf(entry -> !columnsList.contains(entry.getKey()));

    }

    // {"before":null,"after":{"source_table":"base_trademark","sink_table":"dim_base_trademark","sink_columns":"id,tm_name","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103141349,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1662103141349,"transaction":null}
    // {"before":null,"after":{"source_table":"aa","sink_table":"aaa","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103238000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":801824,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1662103238827,"transaction":null}
    // {"before":{"source_table":"aa","sink_table":"aaa","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"after":{"source_table":"aa","sink_table":"abb","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103282000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":802139,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1662103282838,"transaction":null}
    // {"before":{"source_table":"aa","sink_table":"abb","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103312000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":802473,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1662103312677,"transaction":null}
    //TODO 处理广播流配置数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
//        System.out.println(jsonStr);
        // 为了处理属性，将jsonStr 转换为 jsonObj
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        // 获取对配置表的操作类型 r 读 | c新增 |  d删除 |  u修改
        String op = jsonObject.getString("op");

        // 读取广播流中的状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        // 从广播流中的状态 判断 变更数据 对 配置表的操作类型
        if ("d".equals(op)) {
            // 如果对配置表进行删除操作，从广播状态中将配置信息删掉
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            broadcastState.remove(before.getSourceTable());
        } else {
            // 如果对配置表进行 读取 添加 修改 将最新信息添加到广播状态中
            TableProcess after = jsonObject.getObject("after", TableProcess.class);

            // 获取输出到phoenix的维度表的表名
            String sinkTable = after.getSinkTable();
            // 获取输出到phoenix的维度表的字段
            String sinkColumns = after.getSinkColumns();
            // 获取维度表的主键
            String sinkPk = after.getSinkPk();
            // 获取在phoenix中建立维度表的 建表扩展语句
            String sinkExtended = after.getSinkExtend();

            // TODO 在phoenix中提前创建维度表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtended);

            // 将配置信息同步到 广播状态中
            broadcastState.put(after.getSourceTable(), after);
        }

    }

    /**
     * TODO 建表拼接: 注意空值处理，拼接建表语句时候注意空格
     *
     * @param tableName
     * @param columns
     * @param pk
     * @param ext
     */
    /*
    create table if not exists 表空间.表名(
            字段1 varchar primary key,
            字段2 varchar,
            字段3 varchar
            )
    */
    private void checkTable(String tableName, String columns, String pk, String ext) {
        // 拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " (");

        // 获取列字段组成的 数组
        String[] columnArr = columns.split(",");
        // 建表语句时的空值处理
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }

        // TODO 完成建表字段的拼接功能
        for (int i = 0; i < columnArr.length; i++) {
            // 判断是否为主键
            if (columnArr[i].equals(pk)) {
                createSql.append(columnArr[i] + " varchar primary key");
            } else {
                createSql.append(columnArr[i] + " varchar");
            }

            // 判断是否是最后一个
            if (i < columnArr.length - 1) {
                createSql.append(",");
            }

        }
        createSql.append(")" + ext);
        System.out.println("在phoenix中的建表语句为: " + createSql.toString());


        Connection conn = null;
        // TODO 从德鲁伊连接池中获取 数据库连接对象
        DruidDataSource druidDataSource = DruidDSUtil.createDataSource();
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // TODO 执行建表语句
        PhoenixUtil.executeSql(createSql.toString(), conn);
    }

    /**
     * TODO 执行建表语句主方法
     * @param sql
     */
    /*private void executeSql(String sql){
        Connection conn = null;
        PreparedStatement prepStmt = null;
        try {
            // 1. 注册驱动
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            // 2. 获取连接 --> 将phoenix连接地址告诉connection来获取连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_URL);
            // 3. 准备数据库操作对象
            prepStmt = conn.prepareStatement(sql);
            // 4. 执行sql
            prepStmt.execute();
            // 5. 处理结果集

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // 6. 释放资源
            if(prepStmt != null){
                try {
                    prepStmt.close();
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
    }*/
}
