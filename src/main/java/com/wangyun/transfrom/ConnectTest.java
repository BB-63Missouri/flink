package com.wangyun.transfrom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/*
    @author Missouri
    @data 2021-07-19
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<Integer> d1 = env.fromElements(2, 4, 6, 8);
        DataStreamSource<Double> d2 = env.fromElements(1.0, 3.0, 5.0, 7.0);

        ConnectedStreams<Integer, Double> d12 = d1.connect(d2);
        d12
                .map(new CoMapFunction<Integer, Double, Object>() {
                    @Override
                    public Object map1(Integer value) throws Exception {
                        return "int:" + value ;
                    }

                    @Override
                    public Object map2(Double value) throws Exception {
                        return "double:"+ value ;
                    }
                })
                .print();
                

        env.execute();
    }
}
