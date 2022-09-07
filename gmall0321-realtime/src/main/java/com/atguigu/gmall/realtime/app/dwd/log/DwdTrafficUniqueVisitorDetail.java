package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName: DwdTrafficUniqueVisitorDetail
 * @Description: TODO 流量域独立访客 事务事实表
 * @Author: fanfan
 * @DateTime: 2022年09月06日 11时51分
 * @Version: v1.0
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关的设置
        // 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // 设置job取消之后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置两个检查点之间 最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/checkpoint/");
        // 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // TODO 3. 从kafka主题: dwd_traffic_page_log 读取日志数据，封装为流
        // 3.1 声明消费的主题 和 消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_uv_count_group";
        // 3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageLogkafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        // 3.3 消费数据，封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(pageLogkafkaConsumer);

        // TODO 4. 对读取的数据进行类型转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
        // SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 5. 按照mid进行分组
        // KeyedStream<ElementType, KeyType>
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 6. 使用Flink的状态编程，过滤出独立访客
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {
            // 上一次访问日期的值状态变量
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // TODO 值状态变量的初始化
                //      注意点: 针对每个设备id设置的 状态变量的保存周期为 1天
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>(
                        "lastVisitDateState",
                        String.class
                );
                // 值状态变量保存1天
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

            }

            /**
             * 思路： 1. 如果用户行为日志中 上级访问页面id为空，说明用户是当天的独立新访客
             * `````` 2. 如果当前用户的行为日志中，当前设备的上一次访问日期为空，说明是新访客
             *             如果当前用户的行为日志中，当前设备的上一次访问日期 不 等于 当前这一天的访问日期，也说明用户是新访客
             * @param jsonObj
             * @return
             * @throws Exception
             */
            @Override
            public boolean filter(JSONObject jsonObj) throws Exception {
                // 获取上级页面ID
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                // 获取当前设备的上次访问日期
                String lastVisitDate = lastVisitDateState.value();
                // 获取当前设备的访问时间
                String currVisitDate = DateFormatUtil.toDate(jsonObj.getLong("ts"));

                if (lastPageId != null) {
                    return false;
                }

                if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(currVisitDate)) {
                    // 不要忘记更新值状态变量的值
                    lastVisitDateState.update(currVisitDate);
                    return true;
                }
                return false;
            }
        });

        // 测试:
        filterDS.print(">>>>");
        // TODO 7. 将过滤出的独立访客输出到kafka的主题中
        filterDS
                .map(jsonObj -> JSON.toJSONString(jsonObj))
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_unique_visitor_detail"));

        env.execute();
    }
}
