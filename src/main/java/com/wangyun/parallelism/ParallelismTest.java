package com.wangyun.parallelism;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class ParallelismTest {
    /*
如何控制操作链:

1. .startNewChain()
    从当前算子开启一个新链

2. .disableChaining()
    当前算子不会和任何的算子组成链

3.env.disableOperatorChaining();
    全局禁用操作链



给算子设置并行度:
1. 在配置文件中 flink.yaml
    parallelism.default: 1

2. 在提交job的时候通过参数传递
    -p 3

3. 通过执行环境来设置并行度
    env.setParallelism(1);

4. 单独的给每个算子设置并行度





 */
    /*public static void main(String[] args) throws Exception {
        //1创建流式执行环境
        //含参的创建
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        //2.读取文件
        //3对流作转换
        SingleOutputStreamOperator<Tuple2<String, Long>> rt = environment
                .socketTextStream("hadoop162", 9999)
                .flatMap((String line, Collector<String> out) -> {
                    for (String word : line.split(" ")
                    ) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(k -> k.f0)
                .sum(1);
        //4输出结果
        rt.print();
        //5启动执行job
        environment.execute();
    }
*/
    public static void main(String[] args) throws Exception {
//1创建流式执行环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port",8081);
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//2.读取文件
//3对流作转换
        SingleOutputStreamOperator<Tuple2<String, Long>> rt = executionEnvironment
                .socketTextStream("hadoop162", 9999)
                .flatMap((String line, Collector<String> out) -> {
                    for (String word : line.split(" ")
                    ) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG)).setParallelism(3)
                .keyBy(k -> k.f0)
                .sum(1);
//4输出结果
        rt.print();
//5启动执行job
        executionEnvironment.execute();
    }
}