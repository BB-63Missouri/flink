package com.wangyun.wordCount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnBoundedData {
    //nc -lk 9999端口号 前面为命令启动
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.从数据源获取流

        //3.对流作各种转换
        SingleOutputStreamOperator<Tuple2<String, Long>> rean = evn.socketTextStream("hadoop162", 9999)
                //.flatMap((FlatMapFunction<String, Collection<String> >)(line,out) -> { })
                .flatMap((FlatMapFunction<String, String>) (line, out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map((MapFunction<String, Tuple2<String, Long>>) word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) t -> t.f0)
                .sum(1);
        //4.输出结果
        rean.print();
        //5.启动执行环境
        evn.execute();
    }
}
