package com.atguigu.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello world", "hello flink")
                .flatMap(
                        (String value, Collector<WordWithCount> out) -> {
                            // 使用空格切分文本
                            String[] arr = value.split(" ");
                            // 向下游发送数据
                            for (String s : arr) {
                                // 使用collect方法向下游发送数据
                                out.collect(new WordWithCount(s, 1L));
                            }
                        }
                )
                // 类型被擦除为Collector<Object> out
                // 所以需要使用returns方法来对flatMap的输出类型做一个注解
                .returns(Types.POJO(WordWithCount.class))
                .keyBy(r -> r.word)
                .reduce((WordWithCount value1, WordWithCount value2) -> new WordWithCount(value1.word, value1.count + value2.count))
                .print();

        env.execute();
    }

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
