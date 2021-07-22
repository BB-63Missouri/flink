package com.wangyun.state;

import com.wangyun.bean.WaterSensor;
import com.wangyun.util.ToListUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author Missouri
 * @date 2021/7/21 18:14
 */
//获取水位前三
public class Key_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",20000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env
                .socketTextStream("hadoop162",9999)
                .map(value -> {
                    String[] words = value.split(",");
                    return new WaterSensor(words[0],Long.valueOf(words[1]),Integer.valueOf(words[2]) );
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState",Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        //将水位值存入状态，之后一起取所有水位排序
                        listState.add(value.getVc());
                        List<Integer> list = ToListUtil.toList(listState.get());
                        //用Comparator的静态方法按高到低排序reverseOrder高-低
                        list.sort(Comparator.reverseOrder());
                        //当list超过3时去掉后面的
                        if (list.size() > 3){
                            //remove 从索引位置开始删
                            list.remove(list.size() - 1);
                    }
                    }
                })
                .print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
