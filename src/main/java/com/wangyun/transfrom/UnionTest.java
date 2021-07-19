package com.wangyun.transfrom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Missouri
 * @Date 2021-7-19
 */
//union只是两个流拼接起来，一个流输入完再到另一个流输入，设置1并行度得出
public class UnionTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> d1 = env.fromElements(2, 4, 6, 8);
        DataStreamSource<Double> d2 = env.fromElements(1.0, 3.0, 5.0, 7.0);
        DataStreamSource<Integer> d3 = env.fromElements(1, 3, 5, 7);
        //union必须是两个连接同类型
        //DataStream<Integer> d12 = d1.union(d2);

        DataStream<Integer> d13 = d1.union(d3);
        d13.print();


        env.execute();

    }
}
