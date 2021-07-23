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
public class CEP_Loop {
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
                }).times(2,3)
                //.times(n)循环两次,相当于在后面接着共有n次
                //.time(n,m)后面接着n到m此都可以,不需要相连，只要后面有
                //.timesOrMore(n)n此以上，包含 不允许在规则最后，空耗资源
                //.oneOrMore()一次以上，相当于timesOrMore(1)，同上
                ;
        //把模式运用在流上，获取模式流,CEP.pattern()方法传入流和规则
        PatternStream<WaterSensor> sp = CEP.pattern(watermarks, pattern);
        //从模式匹配获取数据，模式流用select方法 Object::toString  map -> map.toString()这两个为啥能转
        sp.select((PatternSelectFunction<WaterSensor, String> )Object::toString
        ).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
