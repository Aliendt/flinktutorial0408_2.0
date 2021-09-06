package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// word count
// 数据来自socket
public class Example2 {
    // 不要忘记抛出异常！
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行任务的数量为1
        env.setParallelism(1);

        // 从数组读取数据源
        DataStreamSource<String> stream = env.fromElements("hello world", "hello flink");

        // map操作 s => (s, 1)
        // map语义：针对列表中的每一个元素输出一个元素
        // flatMap语义：针对列表中的每一个元素输出0个、1个或者多个元素
        // 都是无状态算子
        SingleOutputStreamOperator<WordWithCount> mappedStream = stream
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        // 使用空格切分文本
                        String[] arr = value.split(" ");
                        // 向下游发送数据
                        for (String s : arr) {
                            // 使用collect方法向下游发送数据
                            out.collect(new WordWithCount(s, 1L));
                        }
                    }
                });

        // shuffle操作
        KeyedStream<WordWithCount, String> keyedStream = mappedStream
                .keyBy(new KeySelector<WordWithCount, String>() {
                    @Override
                    public String getKey(WordWithCount value) throws Exception {
                        // 为数据指定key
                        return value.word;
                    }
                });

        // reduce操作：有状态的算子
        // reduce内部会维护一个累加器，累加器的类型和列表中的元素的类型相同
        // reduce函数的输出是累加器
        // 当第一条数据到来时，此时累加器为空，所以第一条数据直接更新累加器
        // 当第N条数据到来时，和累加器进行聚合，并将累加器输出
        SingleOutputStreamOperator<WordWithCount> result = keyedStream
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        // 定义的是输入元素和累加器的聚合逻辑
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        result.print();

        // 执行程序
        env.execute();
    }

    // POJO Class
    // 1. 类必须是公有类
    // 2. 所有字段必须是公有的
    // 3. 必须有空构造器
    public static class WordWithCount {
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
