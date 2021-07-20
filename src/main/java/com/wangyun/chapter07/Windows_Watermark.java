package com.wangyun.chapter07;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 8:34
 */
public class Windows_Watermark {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //不在这里加个WaterSensor会报错，在开头放才能定下类型，不然和后面冲突，但为什么不在后面要在开头加泛型
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    //确定水印的时间戳
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor,
                                                                 long l) {
                                        //快照类似，安排水印的周期,返回单位必须是毫秒
                                        return waterSensor.getTs() * 1000;
                                    }
                                })
                                //设置水印最长等待时间，间隔这个时间后必须照一个水印
                                .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //设置水印的最长等待时间，延迟关闭窗口
                .allowedLateness(Time.seconds(5))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        List<WaterSensor> list = ToListUtil.toList(elements);
                        out.collect(key + "  " + list + "  " + ctx.window());
                    }
                })
                .print();

        env.execute();
    }
}
