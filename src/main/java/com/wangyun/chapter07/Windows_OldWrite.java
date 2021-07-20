package com.wangyun.chapter07;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/20 8:35
 */
public class Windows_OldWrite{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        env
                .socketTextStream("hadoop162",9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws,out)-> ws.getTs() * 1000)
                        .withIdleness(Duration.ofSeconds(20))
                )
                .keyBy(WaterSensor::getId)
                //过时的时间使用默认语义
                .timeWindow(Time.seconds(5))
                //.timeWindow(Time.seconds(5), Time.seconds(2))

                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> out) throws Exception {
                        List<WaterSensor> list = ToListUtil.toList(elements);

                        out.collect(key + "  " + elements + "  " + ctx.window());

                    }
                })
                .print();

        env.execute();
    }
}
