package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName: DimApp @Description: TODO DIM层维度数据的处理 @Author: fanfan @DateTime: 2022年09月02日
 * 12时30分 @Version: v1.0
 */
public class DimApp {
  public static void main(String[] args) throws Exception {
    // TODO 1. 基本环境准备
    // 1.1 指定流处理的环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 1.2 设置并行度
    env.setParallelism(4);

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
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
    // 2.5 设置重启策略
    // 指定时间间隔内(30天)的最大重新启动次数为3次
    // 两次重启之间的时间间隔为 3s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
    // 2.6 设置状态后端
    // 将状态作为内存中的对象进行管理，将本地状态存储在任务管理器的JVM堆上，而将检查点文件存储在作业管理器的内存中
    env.setStateBackend(new HashMapStateBackend());
    env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/checkpoint/");
    // env.getCheckpointConfig().setCheckpointStorage("file:///E:/gmall/ck");

    // 2.7 指定操作hadoop的用户atguigu
    System.setProperty("HADOOP_USER_NAME", "atguigu");

    // TODO 3. 从kafka对应的 topic_db_realtime 中读取数据
    // 声明主题 和 消费者组
    String topic = "topic_db_realtime";
    String groupId = "dim_app_group";
    // Properties prop = new Properties();
    // prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
    // prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    // 创建消费者对象
    FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
    // 消费数据，封装为 kafkaStrDS 流
    DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

    // TODO 4. 对读取的数据进行类型转换 jsonStr -> jsonObj
    SingleOutputStreamOperator<JSONObject> jsonObjDS =
        kafkaStrDS.map(
            new MapFunction<String, JSONObject>() {
              @Override
              public JSONObject map(String in) throws Exception {
                return JSON.parseObject(in);
              }
            });
    // SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(jsonStr ->
    // JSON.parseObject(jsonStr));
    // 方法的默认调用
    // SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

    // TODO 5. 对流中的数据进行简单的ETL
    SingleOutputStreamOperator<JSONObject> filterDS =
        jsonObjDS.filter(
            new FilterFunction<JSONObject>() {
              // 清洗规则:
              // 1.过滤掉不完整的json格式数据
              // 2. 过滤掉maxwell 首日历史数据全量同步的数据
              @Override
              public boolean filter(JSONObject in) throws Exception {
                try {
                  // 尝试获取完整的一条记录中的 data字段的json字串
                  in.getString("data");
                  if ("bootstrap-start".equals(in.getString("type"))
                      || "bootstrap-complete".equals(in.getString("type"))) {
                    return false;
                  }
                  return true;
                } catch (Exception e) {
                  e.printStackTrace();
                  return false;
                }
              }
            });

    // TODO 6. 使用FlinkCDC 读取配置表
    MySqlSource<String> mysqlSource =
        MySqlSource.<String>builder()
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
    DataStreamSource<String> mysqlDS =
        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySql Source");


    // TODO 7. 将读取到的配置信息进行广播 --> 变成广播流
    MapStateDescriptor<String, TableProcess> mapStateDescriptor =
        new MapStateDescriptor<>("map-state", Types.STRING, Types.POJO(TableProcess.class));
    BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);

    // TODO 8. 将主流 和 广播流进行关联
    BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

    // TODO 9. 对关联后的流数据 进行处理  (针对没有键控的流，使用process来进行处理)
    //  -- 得到维度数据流
    SingleOutputStreamOperator<JSONObject> dimDS =
        connectDS.process(new TableProcessFunction(mapStateDescriptor));

    // TODO 10. 将维度数据写入到phoenix表中
    dimDS.addSink(new DimSinkFunction());

    env.execute();
  }
}
