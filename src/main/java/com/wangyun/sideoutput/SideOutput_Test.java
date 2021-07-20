package com.wangyun.sideoutput;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.OutputStream;
import java.time.Duration;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 19:51
 */
public class SideOutput_Test {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest,port",20000);

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
                                //混乱时间
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((input, out) -> input.getVc() * 1000)
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                //在设置迟到时间后设置侧外输出流，调用方法创建标签，为流名称
                .sideOutputLateData(new OutputTag<WaterSensor>("outStream"){})
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        List<WaterSensor> list = ToListUtil.toList(elements);
                        out.collect(key + "  " + list + "  " + ctx.window());
                    }
                });

        result.print();
        //获取侧输出流
        result.getSideOutput(new OutputTag<WaterSensor>("outStream"){}).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
