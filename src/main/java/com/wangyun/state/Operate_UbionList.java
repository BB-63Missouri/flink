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

/**
 * @author Missouri
 * @date 2021/7/21 18:12
 */
public class Operate_UbionList {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 20000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.enableCheckpointing(1000); // 每s做一次Checkpoint

        env
                .socketTextStream("hadoop162", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .flatMap(new MyUnionFunction())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class MyUnionFunction implements FlatMapFunction<String,String>, CheckpointedFunction {
        //存储状态
        private ListState<String> statelist;
        //存储数据状态列表
        private ArrayList<String> list = new ArrayList<>();


        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //更新快照
            statelist.update(list);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("程序初始化状态");
            //获取初始化状态
            statelist= context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("UnionListState",String.class));

            //存储状态列表
            Iterable<String> words = statelist.get();
            for (String word : words) {
                list.add(word);
            }
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            list.addAll(Arrays.asList(value.split(" ")));
        }
    }
}
