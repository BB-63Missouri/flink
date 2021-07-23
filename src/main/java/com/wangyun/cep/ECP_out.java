package com.wangyun.cep;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Missouri
 * @date 2021/7/23 19:09
 */
//用来排除超时的数据，一般用于时间控制。在设计规则的时候加入时间限制，在获取数据时将过期的数据分流出去
public class ECP_out {
    public static void main(String[] args) {
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
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );
        //定义模式规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
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
                        return "sensor_2".equals(value);
                    }
                })
                .within(Time.seconds(2));

        //合成模式流
        PatternStream<WaterSensor> sp = CEP.pattern(waterSensorStream, pattern);

        //获取模式流的数据
        SingleOutputStreamOperator<String> end = sp.select(
                new OutputTag<String>("dataout"),
                (PatternTimeoutFunction<WaterSensor, String>) (pattern12, timeoutTimestamp) -> pattern12.toString(),
                (PatternSelectFunction<WaterSensor, String>) Object::toString);
        end.print("result");
        end.getSideOutput(new OutputTag<String>("dataout"){}).print("dataout");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
