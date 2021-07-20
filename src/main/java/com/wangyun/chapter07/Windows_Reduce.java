package com.wangyun.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/20 8:32
 */
public class Windows_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop162",9999)
                .flatMap((FlatMapFunction<String, Tuple2<String,Long>>)(input, out)->{
                    for (String word: input.split(" ")){
                        out.collect(Tuple2.of(word,1L));
                    }
                }).returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(x->x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //
                /*.reduce(
                        new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2,
                                                               Tuple2<String, Long> t1) throws Exception {
                                System.out.println("聚合一次");
                                //返回一个元组，stringLongTuple2为迭代器类似的之前的聚合数据，t1这次输入的数据
                                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                            }
                        },
                        //每个窗口关闭的时候执行一次,用作聚合后的说明补充
                        new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window,
                                              //input一定有值，是上面聚合的结果
                                              Iterable<Tuple2<String, Long>> input,
                                              Collector<String> out) throws Exception {
                                Tuple2<String, Long> next = input.iterator().next();
                                out.collect(window+" "+next);
                            }
                        }
                )*/
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2,
                                                               Tuple2<String, Long> t1) throws Exception {
                                System.out.println("聚合一次");
                                //返回一个元组，stringLongTuple2为迭代器类似的之前的聚合数据，t1这次输入的数据
                                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                            }
                        },
                        //每一个元素都是执行一次
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context context,
                                                Iterable<Tuple2<String, Long>> elements,
                                                Collector<String> out) throws Exception {
                                System.out.println("最终输出");
                                Tuple2<String, Long> next = elements.iterator().next();
                                out.collect("key:"+key+",windows"+context.window()+" "+next);
                            }
                        }
                )
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
