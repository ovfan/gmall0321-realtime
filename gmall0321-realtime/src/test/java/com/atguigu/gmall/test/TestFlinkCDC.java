package com.atguigu.gmall.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @ClassName: TestFlinkCDC
 * @Description: TODO DIM实现-从kakfa主题中读取数据
 * @Author: fanfan
 * @DateTime: 2022年09月02日 09时36分
 * @Version: v1.0
 */
public class TestFlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2. 检查点相关设置
        // 2.1 开启检查点
        env.enableCheckpointing(
                5000L, CheckpointingMode.EXACTLY_ONCE); // 开启检查点的时间间隔，设置端到端一致性的 检查点模式 为exactly_once
        // 2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointInterval(60000L); // 设置检查点超时时间为 60s
        // 2.3 设置job取消之后检查点是否保留
        // 在job取消后，保留外部的检查点
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000L);
        // 2.5 设置重启策略
        // 指定时间间隔内(30天)的最大重新启动次数为3次
        // 两次重启之间的时间间隔为 3s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 2.6 设置状态后端
        // 将状态作为内存中的对象进行管理，将本地状态存储在任务管理器的JVM堆上，而将检查点文件存储在作业管理器的内存中
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ff/");

        // 2.7 指定操作hadoop的用户atguigu
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 使用flink-cdc 监控并捕捉 mysql配置表中数据变化
        MySqlSource<String> mysqlSource = MySqlSource
                .<String>builder()
                // 指定cdc连接的 主机和端口号
                .hostname("hadoop102")
                .port(3306)
                // 指定要监控的 数据库 和 数据库下的表
                .databaseList("gmall0321_config")
                .tableList("gmall0321_config.table_process")
                // 指定mysql数据库的用户及密码
                .username("root")
                .password("123456")
                // 指定监控的方式
                //  1. 先扫描下监控的这张表
                //  2. 扫描完以后，立刻切换到最新的binlog，也就是最新的数据那里去。
                .startupOptions(StartupOptions.initial())
                // CDC捕获到的原始数据为SourceRecord,需要将其反序列化之后通过collect收集才能传递给下游
                // 这里需要的反序列化是 将 converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();


        env.execute();
    }
}
