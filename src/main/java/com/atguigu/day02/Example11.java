package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

// 自定义sink
public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3)
                .addSink(new RichSinkFunction<Integer>() {
                    // 每来一条数据调用一次
                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        super.invoke(value, context);
                        System.out.println(value);
                    }
                });

        env.execute();
    }
}
