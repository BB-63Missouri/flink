package com.wangyun.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/21 18:12
 */
public class Operate_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(1000); // 每s做一次Checkpoint

        env
                .socketTextStream("hadoop162", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .flatMap(new MyFlatMapFunction())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //想要用状态，还得实现checkpoint接口
    private static class MyFlatMapFunction implements FlatMapFunction<String,String> , CheckpointedFunction {

        //同一个List存输入的value  <String>放在new后面的显示是objeck
        private List<String> word = new ArrayList();
        //设置一个参数存储状态
        private ListState<String> statelist;
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            //秀啊，这个idea，这样子来优化
            word.addAll(Arrays.asList(value.split(" ")));
            out.collect(word.toString());

            if (value.contains("a")){
                throw new RuntimeException();
            }
        }

        //保存状态
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //将这次的word状态更新
            // 获取算子状态中的列表状态
            statelist.update(word);
        }
        //初始化状态
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //initializeState程序启动的时候执行，跟并行符有关，几个就启动几次
            System.out.println("状态开启");
            //get()获取之前的状态  ListStateDescriptor参数第一个是名字，第二个是依据方法，采用class获取
            statelist = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("ListState",String.class));

            Iterable<String> st = statelist.get();
            for (String out : st) {
                //将获取的状态列表放入当前的列表中状态列表
                word.add(out);
            }
        }
    }

}
