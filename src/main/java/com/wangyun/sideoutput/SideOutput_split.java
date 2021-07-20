package com.wangyun.sideoutput;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 20:21
 */
public class SideOutput_split {
    public static void main(String[] args) {
        //创建一个流，准备分流
        OutputTag<WaterSensor> tag = new OutputTag<WaterSensor>("alert"){} ;

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((input, l) -> input.getVc() * 1000)
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(WaterSensor::getId)
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        if (value.getVc() <20 ){
                            out.collect(value+ "数据正常...");
                        }else {
                            ctx.output(tag,value);
                        }
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<WaterSensor>("outStream"){}).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
