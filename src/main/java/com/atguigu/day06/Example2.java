package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 滚动窗口大小是1分钟，计算窗口的pv
// 每隔1秒钟就要输出一次窗口的pv统计值
// 触发器
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new com.atguigu.day02.Example2.CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<com.atguigu.day02.Example2.Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<com.atguigu.day02.Example2.Event>() {
                            @Override
                            public long extractTimestamp(com.atguigu.day02.Example2.Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(r -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<com.atguigu.day02.Example2.Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean key, Context context, Iterable<com.atguigu.day02.Example2.Event> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().getExactSizeIfKnown();
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        out.collect("窗口：" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "的pv是：" + count);
                    }
                })
                .print();

        env.execute();
    }

    public static class MyTrigger extends Trigger<com.atguigu.day02.Example2.Event, TimeWindow> {
        @Override
        public TriggerResult onElement(com.atguigu.day02.Example2.Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每来一条数据触发一次执行
            // 状态变量是一个标志位，为了实现当窗口中的第一条数据到来时，每隔10秒钟注册一个定时器
            // 状态变量作用域：当前窗口
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first", Types.BOOLEAN)
            );
            if (isFirstEvent.value() == null) {
                // 注册第一条事件的时间戳的接下来的整数秒的定时器
                // 1234ms
                // 1234 + (1000 - 1234 % 1000) === 2000
                long ts = element.timestamp + (1000L - element.timestamp % 1000L);
                ctx.registerEventTimeTimer(ts); // 注册的是onEventTime方法
                // 标志位置为true，这样第二条数据就不会进入这个分支了
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 处理时间定时器，当处理时间到达`time`时，触发执行
            return TriggerResult.CONTINUE; // 什么都不做
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // 事件时间的定时器，当水位线到达`time`时，触发执行
            if (time < window.getEnd()) {
                ctx.registerEventTimeTimer(time + 10 * 1000L); // 注册的还是onEventTime方法
                return TriggerResult.FIRE; // 触发全窗口聚合函数中的process方法执行
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            // 当窗口闭合时，触发执行
            // 清空状态变量，单例
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("is-first", Types.BOOLEAN)
            );
            isFirstEvent.clear();
        }
    }
}
