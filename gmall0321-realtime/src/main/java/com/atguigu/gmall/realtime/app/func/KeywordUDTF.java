package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: KeywordUDTF
 * @Description: TODO 自定义UDTF函数 （一进多出）
 * @Author: fanfan
 * @DateTime: 2022年09月14日 08时02分
 * @Version: v1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
