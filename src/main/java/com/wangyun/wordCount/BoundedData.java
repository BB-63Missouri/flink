package com.wangyun.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
/*
//1创建流式执行环境
//2.读取文件
//3对流作转换
//4输出结果
//5启动执行job
 */

public class BoundedData {
    public static void main(String[] args) throws Exception {
        //1创建流式执行环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.createLocalEnvironment();
        evn.setParallelism(1);
        //2.读取文件
        SingleOutputStreamOperator<Tuple2<String, Long>> rean = evn.readTextFile("input/words.txt")
                //3对流作转换
                .flatMap((FlatMapFunction<String, String>) (line, out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                })
                //groupByKey
                .map((MapFunction<String, Tuple2<String, Long>>) word -> Tuple2.of(word,1L))
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
                .sum(1);
        //4输出结果
        rean.print();
        //5启动执行job
        evn.execute();
    }

}
