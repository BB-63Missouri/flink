package com.wangyun.cep;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author Missouri
 * @date 2021/7/23 14:52
 */
public class CEP_Where_Until {

    public static void main(String[] args) {

        //1.获取流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> watermarks = env
                .readTextFile("input/sensor.txt")
                .map(value -> {
                    String[] words = value.split(",");
                    return new WaterSensor(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.getVc()))
                );
        //定义规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                //先定义类型
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
               .timesOrMore(2)
                //until和timesOrMore以及onesOrMore连续用，为停止循环
                .until(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc()>20;
                    }
                });
        //把模式运用在流上，获取模式流,CEP.pattern()方法传入流和规则
        PatternStream<WaterSensor> sp = CEP.pattern(watermarks, pattern);
        //从模式匹配获取数据，模式流用select方法
        // Object::toString  map -> map.toString()这两个为啥能转:rambda表达式匿名内部类简化，返回值调用月方法。
        sp.select((PatternSelectFunction<WaterSensor,String>)Object::toString).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
