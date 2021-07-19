package com.wangyun.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Windows_Sliding {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.socketTextStream("hadoop162",9999)
                .flatMap((FlatMapFunction<String, Tuple2<String,Long>>)(input,out)->{
                    for (String word : input.split(" ")){
                        out.collect(Tuple2.of(word,1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(x->x.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        List<String> word = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            word.add(element.f0);
                        }
                        out.collect("key:"+key+" window:"+context.window()+" word:"+word);
                    }
                }).print();


        env.execute();
    }
}
