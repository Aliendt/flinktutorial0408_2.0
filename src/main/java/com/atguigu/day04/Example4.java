package com.atguigu.day04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 2000L);
                        ctx.emitWatermark(new Watermark(5000L));
                        ctx.collectWithTimestamp("a", 1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            out.collect("迟到数据来了，" + value + "," + ctx.timestamp());
                        } else {
                            out.collect("数据没有迟到，" + value + "," + ctx.timestamp());
                        }
                    }
                })
                .print();

        env.execute();
    }
}
