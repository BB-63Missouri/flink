package com.wangyun.chapter07;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Missouri
 * @date 2021/7/20 8:33
 */
public class Windows_Aggregate {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("hadoop162",9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                })
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<WaterSensor, Tuple2<Integer, Long>, Double>() {
                                //初始化创建一个累加器,每一个key创造一次
                               @Override
                               public Tuple2<Integer, Long> createAccumulator() {
                                   return Tuple2.of(0,0L);
                               }
                                //累加:
                               @Override
                               public Tuple2<Integer, Long> add(WaterSensor waterSensor, Tuple2<Integer, Long> integerLongTuple2) {
                                   return Tuple2.of(integerLongTuple2.f0+waterSensor.getVc(), integerLongTuple2.f1+1);
                               }
                                //关闭窗口的时候调用，输出结果
                               @Override
                               public Double getResult(Tuple2<Integer, Long> integerLongTuple2) {
                                   return integerLongTuple2.f0 * 1.0 / integerLongTuple2.f1;
                               }
                                //合并两个累加器，只有回话窗口才会有
                               @Override
                               public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> integerLongTuple2, Tuple2<Integer, Long> acc1) {
                                   return Tuple2.of(integerLongTuple2.f0+acc1.f0, integerLongTuple2.f1+acc1.f1);
                               }
                           }
                )
                .print();
        env.execute();

    }
}
