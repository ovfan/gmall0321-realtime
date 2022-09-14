package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @ClassName: MyKafkaUtil
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年09月02日 16时30分
 * @Version: v1.0
 */
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // 获取消费者对象
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        // kafka配置 (主要配置 连接kafka集群的地址 和 消费者组)
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 注意: 在使用FlinkKafkaConsumer时,如果使用SimpleStringSchema ,kafka来的消息如果为null，会报错，所以需要我们自己实现kafka反序列化的规则
        // FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props)
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            /**
             * TODO 实现反序列化的具体逻辑
             * @param consumerRecord
             * @return
             * @throws Exception
             */
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                // 如果消费的一条记录值不为null，将其返回
                if (consumerRecord.value() != null && consumerRecord !=null) {
                    return new String(consumerRecord.value());
                }
                return null;
            }

            /**
             * TODO 指定类型
             * @return
             */
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);

        return kafkaConsumer;
    }
    //获取kafka的生产者对象
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");

        //注意：通过以下方式创建的生产者对象 默认Semantic.AT_LEAST_ONCE，不能保证精准一次
        // FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("hadoop202:9092", "dirty_data", new SimpleStringSchema());
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("default_topic", new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String str, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic, str.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return kafkaProducer;
    }

    /**
     * Kafka-Source DDL语句
     * @param topic 数据源主题
     * @param groupId 消费者组
     * @return 凭借好的Kafka数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic,String groupId){
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";

    }

    /**
     * 最终结果写入kafka的 ext拓展语句 --- Kafka-Sink DDL语句
     * @param topic 输出到kafka的目标主题
     * @return 拼接好的kafka-Sink DDL语句
     */
    public static String getUpsertKafkaDDL(String topic){
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
