package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class Example6 {
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
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
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

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Tuple2<String, Long>> {
        @Override
        public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Tuple2<String, Long>>() {
                private long delay = 0L; // 最大延迟时间
                private long maxTs = -Long.MAX_VALUE + delay + 1L; // 观察到的最大时间戳
                @Override
                public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                    // 每来一条数据，试图更新一次观察到的最大时间戳
                    maxTs = Math.max(maxTs, event.f1);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // 周期性的向数据流中插入水位线
                    output.emitWatermark(new Watermark(maxTs - delay - 1L));
                }
            };
        }

        @Override
        public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                    return element.f1; // 指定时间戳是哪一个字段
                }
            };
        }
    }
}
