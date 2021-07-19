package com.wangyun.transfrom;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Missouri
 * @Date 2021-7-19
 */
public class Process_KeyBy {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        DataStreamSource<WaterSensor> s1 = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 4L, 40),
                new WaterSensor("sensor_2", 3L, 30),
                new WaterSensor("sensor_1", 2L, 40),
                new WaterSensor("sensor_3", 2L, 40),
                new WaterSensor("sensor_1", 5L, 50)
        );

        s1
                .keyBy(WaterSensor::getId)
                //得先分组，不然出错
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    Map<String,Integer> idToSum = new HashMap<>();
            @Override
            public void processElement(WaterSensor value,
                                       Context ctx,
                                       Collector<Integer> out) throws Exception {
                Integer orDefault = idToSum.getOrDefault(ctx.getCurrentKey(), 0);
                orDefault += value.getVc();
                out.collect(orDefault);
                idToSum.put(ctx.getCurrentKey(),orDefault);
            }
        }).print();

        env.execute();
    }
}
