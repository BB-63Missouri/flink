package com.wangyun.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/21 18:14
 */


public class Operate_Broad {
/*
广播状态:
    广播状态的本质其实是一个Map集合

    先有一个广播流, 广播流再和普通的数据流进行connect
 */
public static void main(String[] args) {
    Configuration conf = new Configuration();
    conf.setInteger("rest.port", 20000);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    env.setParallelism(2);

    // 两个流: 一个数据流, 正常的数据. 另外一个流是: 控制流, 用来动态控制数据流中的数据的处理逻辑，名称只是用途不同，创造的都是普通的流
    DataStreamSource<String> controlStream = env.socketTextStream("hadoop162", 9999);
    DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);

    // 1. 把控制流做成广播流
    //调用broadcast方法，将String name, Class<UK> keyClass, Class<UV> valueClass三个参数传入
    MapStateDescriptor<String, String> bdDesc = new MapStateDescriptor<>("broadstate", String.class, String.class);
    BroadcastStream<String> broadcastStream = controlStream.broadcast(bdDesc);

    // 2. 用数据流去connect广播流
        dataStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    // 处理数据流的数据
                    @Override
                    public void processElement(String value,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        //获取广播流，key是名称，v是
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(bdDesc);
                        String v = broadcastState.get("with");
                        if ("1".equals(v)) {
                            out.collect("使用1号逻辑处理数据");
                        } else if ("2".equals(v)) {
                            out.collect("使用2号逻辑处理数据");
                        } else {
                            out.collect("使用默认逻辑处理数据");
                        }
                    }
                    // 处理广播流的数据
                    @Override
                    public void processBroadcastElement(String value,
                                                        Context ctx,
                                                        Collector<String> out) throws Exception {
                        //获取广播流
                        BroadcastState<String, String> bdState = ctx.getBroadcastState(bdDesc);
                        // 把每天数据放入到广播状态
                        bdState.put("with",value);
                    }
                })
                .print();

    try {
        env.execute();
    } catch (Exception e) {
        e.printStackTrace();
    }
}
}
