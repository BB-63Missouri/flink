package com.wangyun.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
//Session是设定间隔(gap)来关闭窗口,也可以使用动态的gap来设置
//动态设置要调用withDynamicGap，创建SessionWindowTimeGapExtractor匿名内部类
public class Windows_Session {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop102",9999)
                .flatMap((FlatMapFunction<String, Tuple2<String,Long>>)(input,out)->{
                    for (String word: input.split(" ")){
                        out.collect(Tuple2.of(word,1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(x->x.f0)
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))

                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
                    //TimeWindow(long start, long end) end =currentProcessingTime（处理时间，即start） + sessionTimeout(出入element获取的时间)
                    //输入element获取关闭窗口的间隔时间
                    @Override
                    public long extract(Tuple2<String, Long> element) {
                        return element.f0.length()*1000;
                    }
                }))
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

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
