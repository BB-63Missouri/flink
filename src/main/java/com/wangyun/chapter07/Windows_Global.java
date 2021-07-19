package com.wangyun.chapter07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Windows_Global {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.socketTextStream("hadoop162", 9999)
                .flatMap((FlatMapFunction<String,Tuple2<String,Long>>)(value,out) ->
                        {
                            for (String words: value.split(" ")
                                 ) {
                                out.collect(Tuple2.of(words,1L));
                            }
                        })
                //flatmap算子用rambda表达式需要返回值类型
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(x->x.f0)
                .window(GlobalWindows.create())
                //Sets the Trigger that should be used to trigger window emission.触发器，关闭窗口的，调用trigger,需要创建触发器，也可以自定义

                .trigger(new Trigger<Tuple2<String, Long>, GlobalWindow>() {

                    int count = 0;
                    //来一个数据触发一次方法
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element,
                                                   long timestamp,
                                                   GlobalWindow window,
                                                   TriggerContext ctx) throws Exception {
                        count++;
                        if (count % 3 == 0){
                            //TriggerResult4种属性，fire关闭窗口，purge清空窗口，continue下个输入，fire——and——purge两个兼之
                            return TriggerResult.FIRE_AND_PURGE;
                        }else {
                            return TriggerResult.CONTINUE;
                        }
                    }
                    //处理时间时触发
                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("onProcessingTime");
                        return null;
                    }
                    //处理事件时触发
                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("onEventTime");
                        return null;
                    }
                    //清除状态
                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                //处理窗口一般是用process,传入ProcessWindowFunction
                //new ProcessWindowFunction<Tuple2<String, Long>输入, Object输出, String(key), GlobalWindow>()窗口类型，自己改不了，根据前面的改
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, GlobalWindow>() {
                   //process(K key, final Context context, Iterable<T> input, Collector<R> out)
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<String> out) throws Exception {
                        //Iterable一般是遍历，打印可以用个容器装载再打印
                        List<String> s = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            s.add(element.f0);
                        }
                        out.collect("key="+key+"window+"+context.window()+"words="+s);
                    }
                })
                .print();
        env.execute();
    }
}
