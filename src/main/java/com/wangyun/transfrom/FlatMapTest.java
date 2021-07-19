package com.wangyun.transfrom;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Missouri
 * @Date 2021-7-16
 */
public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<Integer> s1 = env.fromElements(1, 2, 3, 4, 5, 6);
        //lfatMap的rambda参数是一个进去，一个出去,过滤和变化，不是扁平化
        s1.flatMap((FlatMapFunction<Integer, Integer>)(input, out) ->{
            if (input % 2 == 0){
                out.collect(input);
            }
        })
                //只要外部定义输入？
                .returns(Types.INT)
                .print();

        env.execute();

    }
}
