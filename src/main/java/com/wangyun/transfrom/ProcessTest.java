package com.wangyun.transfrom;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Missouri
 * @Date 2021-7-19
 */
public class ProcessTest {
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
                new WaterSensor("sensor_1", 5L, 50)
        );

        s1.process(new ProcessFunction<WaterSensor, Integer>() {
            int sum = 0;
            @Override
            public void processElement(WaterSensor value,
                                       Context ctx,
                                       Collector<Integer> out) throws Exception {
                sum += value.getVc();
                out.collect(sum);
            }
        })
                .print();

        env.execute();
    }
}
