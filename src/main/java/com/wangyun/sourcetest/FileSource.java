package com.wangyun.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Author Missouri
 * @Date 2021-7-16
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);
        DataStreamSource<String> source = environment.readTextFile("hdfs://hadoop162:8020/input/words.txt");

        source.print();

        environment.execute();


    }
}
