package com.wangyun.chapter07;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.eventtime.*;
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
 * @date 2021/7/20 8:35
 */
public class Windows_Watermark_Custor {
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
                        new WatermarkStrategy<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new MyWMG();//直接返回自定义的水印
                            }
                        }
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

    /*public static class MyWMG implements WatermarkGenerator<WaterSensor> {


        long maxTs = 0;

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyWMG.onEvent");
            maxTs = Math.max(maxTs, eventTimestamp);

            output.emitWatermark(new Watermark(maxTs - 3000));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("MyWMG.onPeriodicEmit");
            //output.emitWatermark(new Watermark(maxTs - 3000));

        }
    }*/
    //自定义生成水印的都需要实现WatermarkGenerator,重写两个方法
    public static class MyWMG implements WatermarkGenerator<WaterSensor> {

        long maxTs = 0;

        //乱序流水印中是取已知的时间戳最大值
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            //Watermark创造水印，传时间戳进去 3000毫秒是当乱序时间.
            output.emitWatermark(new Watermark(maxTs - 3000));
        }

        //按照正常都是最后执行
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("onPeriodicEmit");
        }
    }
}
