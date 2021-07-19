package com.wangyun.transfrom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.checkerframework.checker.units.qual.C;

/**
 * @Author Missouri
 * @Date 2021-7-16
 */
public class Map_Rich {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<Integer> d1 = env.fromElements(20, 23, 54, 23, 12, 12, 33, 4, 2, 3);

        //用rich为
        d1.map(new RichMapFunction<Integer, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {


            }

            @Override
            public void close() throws Exception {

            }

            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer * integer;
            }
        })
        .print();
        env.execute();
    }
}
