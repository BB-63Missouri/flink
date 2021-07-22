package com.wangyun.state;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/21 18:15
 */
public class Key_Reduce {
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

                    //创建reduce状态
                    private ReducingState<WaterSensor> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //给sumVcState赋值，getRuntimeContext()获取正在运行的状态
                        sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reduceState", new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                 value1.setVc(value1.getVc()+value2.getVc());
                                 return value1;
                            }
                        }, WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sumVcState.add(value);
                        out.collect(ctx.getCurrentKey() + "的水位和是: " + sumVcState.get().getVc());
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
