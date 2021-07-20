package com.wangyun.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/19 21:25
 */
//countWindow(x,y)可单x，类型都是long，第一个是滚动间隔，第二个参数是步长
    //为依赖数量的分类，元素数量就为long
public class Windows_Count {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.socketTextStream("hadoop162", 9999)
                .flatMap((FlatMapFunction<String, Tuple2<String,Long>>)(value, out) ->
                {
                    for (String words: value.split(" ")
                    ) {
                        out.collect(Tuple2.of(words,1L));
                    }
                })
                //flatmap算子用rambda表达式需要返回值类型
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(x->x.f0)
                .countWindow(5,2)
                //countWindow后面的prpcess的new ProcessWindowFunction 为什么是GlobalWindow类型
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        List<String> words= new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            words.add(element.f0);
                        }
                        out.collect("key:"+ key+",windows:"+context.window()+",words:"+words);
                    }
                }).print();
    }
}
