package com.atguigu.day03;

import com.atguigu.day02.Example2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 增量聚合函数和全窗口聚合函数结合使用
// 增量聚合的结果 -> 全窗口聚合函数
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new Example2.CustomSource())
                .keyBy(r -> r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    // 负责聚合
    public static class CountAgg implements AggregateFunction<Example2.Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L; // 创建累加器
        }

        @Override
        public Long add(com.atguigu.day02.Example2.Event value, Long accumulator) {
            return accumulator + 1L; // 输入元素和累加器的累加的逻辑
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator; // 窗口闭合时输出什么结果
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 负责在聚合结果上面包裹一层窗口信息
    public static class WindowResult extends ProcessWindowFunction<Long, Example1.UserViewCountPerWindow, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<Example1.UserViewCountPerWindow> out) throws Exception {
            // 迭代器中只有一个元素！就是窗口闭合时，增量聚合函数发送过来的计算结果
            Long windowStart = context.window().getStart();
            Long windowEnd = context.window().getEnd();
            Long count = elements.iterator().next(); // 获取迭代器中的元素个数
            out.collect(new Example1.UserViewCountPerWindow(s, count, windowStart, windowEnd));
        }
    }
}
