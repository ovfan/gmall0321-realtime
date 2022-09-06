package com.atguigu.gmall.realtime.bean;
import lombok.Data;
// 使用lombok
// 在需要的实体类中引入相关注解即可 @Data
/**
 * @ClassName: TableProcess
 * @Description: TODO 将flink cdc 监控的表的 得到的数据 jsonStr -> TableProcess Bean 对象 方便后续写入 phoenix hbase 中
 * @Author: fanfan
 * @DateTime: 2022年09月02日 17时33分
 * @Version: v1.0
 */

// 配置表 对应实体类
@Data
public class TableProcess {
    String sourceTable;     // 来源表
    String sinkTable;       // 输出表
    String sinkColumns;     // 输出字段
    String sinkPk;          // 主键字段
    String sinkExtend;    // 建表扩展
}
