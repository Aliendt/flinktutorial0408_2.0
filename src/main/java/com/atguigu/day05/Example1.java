package com.atguigu.day05;

import com.atguigu.day04.Example10;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0408/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                )
                .keyBy(r -> true)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<UserBehavior, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
            // key: itemId
            // value: count
            HashMap<String, Long> hashMap = new HashMap<>();
            for (UserBehavior e : elements) {
                if (!hashMap.containsKey(e.itemId)) {
                    hashMap.put(e.itemId, 1L);
                } else {
                    hashMap.put(e.itemId, hashMap.get(e.itemId) + 1L);
                }
            }

            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (String key : hashMap.keySet()) {
                itemViewCounts.add(new ItemViewCount(key, hashMap.get(key), context.window().getStart(), context.window().getEnd()));
            }

            // 按照浏览量降序排列
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t2, ItemViewCount t1) {
                    return t1.count.intValue() - t2.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("==================================================\n")
                    .append("窗口结束时间：" + new Timestamp(context.window().getEnd()))
                    .append("\n");

            for (int i = 0; i < 3; i++) {
                ItemViewCount currIvc = itemViewCounts.get(i);
                result
                        .append("第" + (i+1) + "名的商品ID是：" + currIvc.itemId + "，浏览量是：" + currIvc.count + "\n");
            }
            result
                    .append("==================================================\n");
            out.collect(result.toString());
        }
    }

    public static class ItemViewCount {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCount() {
        }

        public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

    public static class UserBehavior {
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
