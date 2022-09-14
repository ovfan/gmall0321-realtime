package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: KeywordUtil
 * @Description: TODO IK分词器 分词工具类
 * @Author: fanfan
 * @DateTime: 2022年09月13日 08时49分
 * @Version: v1.0
 * 
 */
public class KeywordUtil {
    /**
     * 返回分词后的 集合
     * @param text
     * @return
     */
    public static List<String> analyze(String text){
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        /**
         * IK分词器构造函数
         * @param input
         * @param useSmart 为true，使用智能分词策略
         *
         * 非智能分词：细粒度输出所有可能的切分结果
         * 智能分词： 合并数词和量词，对分词结果进行歧义判断,结果更准确
         */
        IKSegmenter ik = new IKSegmenter(reader,true);
        try {
            Lexeme lexeme = null;
            while((lexeme = ik.next())!=null){
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }

    // 测试分词效果
    public static void main(String[] args) {
        System.out.println(analyze("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待"));

    }
}
