package com.wangyun.transfrom;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Missouri
 * @Date 2021-7-19
 */

//与shuffle不同的是轮流分配，保证每个并行的负载相近，不会造成数据倾斜。
public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<WaterSensor> s1 = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 4L, 40),
                new WaterSensor("sensor_2", 3L, 30),
                new WaterSensor("sensor_1", 2L, 40),
                new WaterSensor("sensor_3", 2L, 40),
                new WaterSensor("sensor_1", 5L, 50)
        );

        s1.rebalance().print().setParallelism(3);


        env.execute();
    }
}
