package com.wangyun.state;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/21 18:15
 */
public class Key_aggregating {
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

                    //ει ηΆζ
                    private AggregatingState<WaterSensor,Double> aggState;

                    class ThisAvg {
                        Integer sumVc = 0;
                        Long count = 0L;

                        public Double getAvg(){
                            return sumVc * 1.0 / count;
                        }
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<WaterSensor, ThisAvg, Double>
                                        ("aggstate", new AggregateFunction<WaterSensor, ThisAvg, Double>() {
                            @Override
                            public ThisAvg createAccumulator() {
                                return new ThisAvg();
                            }

                            @Override
                            public ThisAvg add(WaterSensor value, ThisAvg accumulator) {
                                //εε€εΆοΌε¨θ°η¨ζΉζ³ζ₯θΏεθ?‘η?η»ζ
                                accumulator.sumVc += value.getVc();
                                accumulator.count += 1;
                                return accumulator;
                            }

                            @Override
                            public Double getResult(ThisAvg accumulator) {
                                return accumulator.getAvg();
                            }
                            //εͺζεθ―ηͺε£ζζ
                            @Override
                            public ThisAvg merge(ThisAvg a, ThisAvg b) {
                                return null;
                            }
                        }, ThisAvg.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        aggState.add(value);
                        out.collect(ctx.getCurrentKey() + " ηεΉ³εζ°΄δ½ζ―: " + aggState.get());
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
