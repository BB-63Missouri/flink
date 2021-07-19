package com.wangyun.sourcetest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class ColletionSource {

    public static void main(String[] args) throws Exception {

        List<String> str = new ArrayList<>();
        str.add("a");
        str.add("b");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        environment.fromCollection(str).print();

        environment.execute();
    }
    //1创建环境

}
