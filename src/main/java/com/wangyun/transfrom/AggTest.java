package com.wangyun.transfrom;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
    @author:Missouri
    @data:2021-7-19
 */
public class AggTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<WaterSensor> d1 = env.fromElements(
                new WaterSensor("sensor_1",1L,10),
                new WaterSensor("sensor_1", 4L, 40),
                new WaterSensor("sensor_2", 3L, 30),
                new WaterSensor("sensor_1", 2L, 40),
                new WaterSensor("sensor_1", 5L, 50)
        );
        d1
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .print();


        env.execute();
    }
}
