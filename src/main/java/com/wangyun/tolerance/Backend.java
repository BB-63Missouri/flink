package com.wangyun.tolerance;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/22 10:33
 */
public class Backend {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //env调用setStateBackend，把三种类型的对象传进去
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
                env
                .socketTextStream("hadoop162",9999)
                .map(value -> {
                    String[] words = value.split(",");
                    return new WaterSensor(words[0],Long.valueOf(words[1]),Integer.valueOf(words[2]) );
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //设置获取上次的值状态
                    private ValueState<Integer> lastState;

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        //从记录的状态获取上次的vc值
                        Integer lastVc = lastState.value();
                        //判断连续两次水位是否超过10
                        if (lastVc != null && value.getVc() > 10 && lastVc > 10){
                            //更新状态，只要状态update当前值就能更新状态
                            lastState.update(value.getVc());
                        }
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
