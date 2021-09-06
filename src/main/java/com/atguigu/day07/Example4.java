package com.atguigu.day07;

import com.atguigu.day02.Example2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// 幂等性写入mysql
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new Example2.CustomSource())
                .addSink(new MyJDBC());


        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<Example2.Event> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop202:3306/sensor",
                    "root",
                    "123456"
            );
            insertStmt = conn.prepareStatement("INSERT INTO clicks (user, url) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE clicks SET url = ? WHERE user = ?");
        }

        @Override
        public void invoke(Example2.Event value, Context context) throws Exception {
            super.invoke(value, context);
            updateStmt.setString(1, value.url);
            updateStmt.setString(2, value.user);
            updateStmt.execute();

            // 如果更新次数为0,则表示之前未插入过user对应的数据
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.user);
                insertStmt.setString(2, value.url);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
