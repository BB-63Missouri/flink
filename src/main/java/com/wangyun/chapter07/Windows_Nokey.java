package com.wangyun.chapter07;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 8:32
 */
public class Windows_Nokey {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop102",9999)
                .flatMap((FlatMapFunction<String, Tuple2<String,Long>>)(input, out)->{
                    for (String word: input.split(" ")){
                        out.collect(Tuple2.of(word,1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                //没有设定keyBy则只能用windowAll开窗口，里面可以设置有关窗口的全部函数
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        List<String> word = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            word.add(element.f0);
                        }
                        //根据环境调用window获取其中的时间戳
                        Date start = new Date(ctx.window().getStart());
                        Date end = new Date(ctx.window().getEnd());
                        out.collect("window=[" + start + "," + end + ")" + "words=" + word);
                    }
                }).print();
    }
}
