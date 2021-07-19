package com.wangyun.transfrom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



/**
 * @Author Missouri
 * @Date 2021-7-17
 */
public class KeyByTest {
    public static void main(String[] args) throws Exception {
        Configuration cof = new Configuration();
        cof.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(cof);
        //配置数据,模拟流数据
        DataStreamSource<Integer> p1 = env.fromElements(1, 13, 11, 2, 3, 6);
        p1
                .keyBy(x -> x%2)
                .print();

        env.execute();


    }
}
