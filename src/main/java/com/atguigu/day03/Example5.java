package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// KeyedProcessFunction的使用
// 定时器的使用
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        // 每来一条数据触发一次执行
                        // 处理时间
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("数据来了：" + value + ", 到达的时间戳是：" + new Timestamp(currTs));
                        // 注册一个20秒钟之后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 2000L);
                        out.collect("注册了一个时间戳是：" + new Timestamp(currTs + 20 * 1000L) + " 的定时器");
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了，触发时间是：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
