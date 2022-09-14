package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: KeywordBean
 * @Description: 关键词统计的实体类
 * @Author: fanfan
 * @DateTime: 2022年09月14日 08时10分
 * @Version: v1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 关键词来源
    private String source;
    // 关键词
    private String keyword;
    // 关键词出现频次
    private Long keyword_count;
    // 时间戳
    private Long ts;
}
