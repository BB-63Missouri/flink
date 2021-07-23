package com.wangyun.cep;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Missouri
 * @date 2021/7/23 18:50
 */
//即在Pattern里面套Pattern，因为优先级的问题，有时候想让部分一起当整体
public class CEP_group {
    public static void main(String[] args) throws Exception {
        // 1. 先有流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]) * 1000,
                                Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        // 2. 定义规则(模式)
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .begin(
                        Pattern
                                .<WaterSensor>begin("s1")
                                .where(new SimpleCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor value) throws Exception {
                                        return "sensor_1".equals(value.getId());
                                    }
                                })
                                .next("s2")
                                .where(new SimpleCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor value) throws Exception {
                                        return "sensor_2".equals(value.getId());
                                    }
                                })
                )
                .times(2)
                .consecutive();

        // 3. 把模式运用在流上-> 得到一个模式流

        PatternStream<WaterSensor> ps = CEP.pattern(waterSensorStream, pattern);
        // 4. 从模式流中取出匹配到的数据
        ps
                .select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                        return pattern.toString();
                    }

                })
                .print();

        env.execute();

    }
}
