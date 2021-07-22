package com.wangyun.state;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/21 18:16
 */

//将WaterSensor改成用元组
public class Key_Aggregating2 {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env
                .socketTextStream("hadoop162", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private AggregatingState<WaterSensor, Double> avgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgState = getRuntimeContext()
                                .getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Long>, Double>(
                                        "avgState",
                                        new AggregateFunction<WaterSensor, Tuple2<Integer, Long>, Double>() {
                                            @Override
                                            public Tuple2<Integer, Long> createAccumulator() {
                                                return Tuple2.of(0, 0L);
                                            }

                                            @Override
                                            public Tuple2<Integer, Long> add(WaterSensor value, Tuple2<Integer, Long> acc) {
                                                return Tuple2.of(acc.f0 + value.getVc(), acc.f1 + 1L);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Integer, Long> acc) {
                                                return acc.f0 * 1.0 / acc.f1;
                                            }

                                            @Override
                                            public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.INT, Types.LONG)

                                ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        avgState.add(value);

                        out.collect(ctx.getCurrentKey() + " 的平均水位是: " + avgState.get());
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
