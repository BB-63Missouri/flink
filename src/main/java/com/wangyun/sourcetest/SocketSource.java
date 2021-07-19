package com.wangyun.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Author Missouri
 * @Date 2021-7-16
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> out = environment.socketTextStream("hadoop162", 9999);
        out.print();
        environment.execute();
    }
}
