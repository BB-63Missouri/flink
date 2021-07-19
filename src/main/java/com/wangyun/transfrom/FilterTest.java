package com.wangyun.transfrom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Missouri
 * @Date 2021-7-16
 */

public class FilterTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment(conf);
        DataStreamSource<Integer> i1 = env.fromElements(1, 2, 3, 4, 5, 6);

        i1
                //ture返回
                .filter(x-> x % 2 == 0)
                .print();
        env.execute();
    }
}
