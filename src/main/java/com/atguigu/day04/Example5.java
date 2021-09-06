package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Tuple2<String, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                        ctx.collect(Tuple2.of("a", 1000L));
                        Thread.sleep(1000L);
                        ctx.collect(Tuple2.of("a", 2000L));
                        Thread.sleep(1000L);
                        ctx.collect(Tuple2.of("a", 3000L));
                        Thread.sleep(1000L);
                        ctx.collect(Tuple2.of("a", 1000L));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            out.collect("迟到" + value);
                        } else {
                            out.collect(value.toString());
                        }
                    }
                })
                .print();

        env.execute();
    }
}
