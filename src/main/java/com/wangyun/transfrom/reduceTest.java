package com.wangyun.transfrom;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Missouri
 * @Date 2021-7-19
 */
public class reduceTest {
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
        // 上次聚合的结果
        d1.keyBy(WaterSensor::getId)
                .reduce((value1,value2)->{
                    System.out.println("计算一次");
                    value2.setVc(value1.getVc()+value2.getVc());
                    return value2;
                })
                .print();
        env.execute();
    }
}
