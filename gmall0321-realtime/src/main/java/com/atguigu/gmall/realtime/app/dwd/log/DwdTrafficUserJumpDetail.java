package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName: DwdTrafficUserJumpDetail
 * @Description: TODO 流量域 用户跳出明细事实表
 * 思路：使用Flink CEP判断是否为跳出行为
 * 跳出是指会话中只有一个页面的访问行为，如果能获取会话的所有页面，只要筛选页面数为 1 的会话即可获取跳出明细数据
 * @Author: fanfan
 * @DateTime: 2022年09月06日 16时30分
 * @Version: v1.0
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 消费数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_group";
        DataStreamSource<String> kafkaPageLogDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));
       /* DataStreamSource<String> kafkaPageLogDS = env.fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"home\"},\"ts\":15000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"detail\"},\"ts\":30000} "
        );*/

        // TODO 对流中数据进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaPageLogDS.map(JSON::parseObject);
        // jsonObjDS.print("--->");

        // TODO 指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        })
        );

        // TODO 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(r -> r.getJSONObject("common").getString("mid"));

        //TODO 核心方法: 使用Flink CEP判断是否为跳出行为
        // 7.1 定义pattern 模式   --> 判断跳出行为
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            /**
             *  第一个条件：判断流中的元素是否是一个新的独立的会话
             * @param jsonObject
             * @return
             */
            @Override
            public boolean filter(JSONObject jsonObject) {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return StringUtils.isEmpty(lastPageId);
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            /**
             *   第二个条件：判断流中的元素是不是另外一种会话
             *      如果10s种以内打开两种会话，第一个条件中的元素属于跳出行为
             * @param jsonObject
             * @return
             */
            @Override
            public boolean filter(JSONObject jsonObject) {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return StringUtils.isEmpty(lastPageId);
            }
        }).within(Time.seconds(10));

        // 7.2 将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        // 7.3 从流中提取数据 (完全匹配 + 超时时间)
        OutputTag<String> timeOutputTag = new OutputTag<String>("timeOutTag") {
        };
        SingleOutputStreamOperator<String> matchDS = patternDS.select(
                timeOutputTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        // 处理超时数据  注意：如果是超时数据，方法的返回值会被放到侧输出流中
                        return pattern.get("first").get(0).toJSONString();
                    }
                },
                new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                        // 处理完全匹配的数据 注意：方法的返回值输出到主流中
                        return pattern.get("first").get(0).toJSONString();
                    }
                }
        );

        // TODO 将完全匹配的数据和超时数据进行合并
        DataStream<String> unionDS = matchDS.union(matchDS.getSideOutput(timeOutputTag));

        // TODO 将跳出行为发送到kafka主题中
        unionDS.print("---->");
        // unionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));
        env.execute();
    }
}
