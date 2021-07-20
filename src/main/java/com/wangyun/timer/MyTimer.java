package com.wangyun.timer;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Missouri
 * @date 2021/7/21 8:38
 */

//这个自定义定时器是在process里改变逻辑
public class MyTimer {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

         env
                .socketTextStream("hadoop162", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //设置混乱时间的
                                .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000)
                )
                 .keyBy(WaterSensor::getId)
                 .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                     //由于多个需要时间戳，提取出来
                     private long timerTs;
                     //判断是否是第一个数据
                     Boolean isFrist = true;
                     //定一个数获取上次的水位值,第一次调用时没有上次水位数进入了第一次的，不进去判断，之后赋值。
                     int lastVc;

                     @Override
                     public void processElement(WaterSensor value,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        if (isFrist){
                            timerTs = ctx.timestamp()+5000;
                            //注册时的时间是定时器的触发时间
                            ctx.timerService().registerEventTimeTimer(ctx.timestamp()+timerTs);
                            //不是第一个上升的就改
                            isFrist =false;
                        }
                        else {
                            if (value.getVc() < lastVc){
                                ctx.timerService().deleteEventTimeTimer(timerTs);
                                //获取当前时间重新注册触发器
                                timerTs = ctx.timestamp()+5000;
                                ctx.timerService().registerEventTimeTimer(timerTs);
                            }else {
                                System.out.println("5秒内水位上升或不变");
                            }
                        }
                        lastVc = value.getVc();
                     }
                 })
                 .print();


                    /*
                    1. 第一条数据来了之后, 注册一个5秒后触发的定时器
                    2. 下一条数据来了之后, 判断与上一条的大小关系
                    3. 如果大于等于上一条, 不用管
                    4. 如果小于上一条则删除定时器


                    5. 定时器如果触发, 重新注册一个新的定时器
                     */


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
