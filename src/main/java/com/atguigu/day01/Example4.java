package com.atguigu.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 并行度的设置
// 算子并行度的优先级高于全局并行度
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .map(r -> Tuple2.of("key", r)).setParallelism(4)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(r -> r.f0)
                .sum(1).setParallelism(2)
                .print().setParallelism(4);

        env.execute();
    }
}
