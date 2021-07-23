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
 * @date 2021/7/23 14:53
 */
public class CEP_Where_Combine {
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
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //            .next("end")  // 严格连续 邻接的，中间没有不匹配的
                   //         .followedBy("end")  // 松散连续 中间可以有不匹配的
                //            .followedByAny("end")  // 非确定性松散连续 ，中间有任何其他的符合的都可以，a1 b1 b2 求ab的 ，则a1 ,b2可以
                //            .notNext("end") 意思是在匹配中找不到不符合的内容，然后丢弃掉。
                //            .notFollowedBy("end") 在中间如果有本次不匹配的模式，这次也要排除
                //notNext(),notFollowedBy()，更像是在符合条件中过滤掉不符合的数据
                .notFollowedBy("end")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                })
                //next忽略两者之间的，
                .next("last")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_3".equals(value.getId());
                    }

                });

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
