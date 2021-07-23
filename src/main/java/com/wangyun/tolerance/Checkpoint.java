package com.wangyun.tolerance;

import com.wangyun.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author Missouri
 * @date 2021/7/22 13:59
 */
public class Checkpoint {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id", "kafkatoflink");
        properties.setProperty("auto.offset.reset", "latest");

        Properties sinkProps = new Properties();
        sinkProps.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        //由于在严格模式中不能超过15分钟过期，而，kafka默认1小时，所以要设置。
        sinkProps.setProperty("transaction.timeout.ms", 14 * 60 * 1000 + "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration())
                .setParallelism(3);
        //设置状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend())
                .getCheckpointConfig().setCheckpointStorage("hdfs://haddoop162:8020/flink/checkpoints/rocksdb");
        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(5000)
        // 高级选项：
        // 设置模式为严格一次 (这是默认值)先获取配置之后设置
        .getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启在 job 中止后仍然保留的 externalized checkpoints 一般
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env
                //加kafkaSource
                .addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));

                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer lastVc = state.value() == null ? 0 : state.value();
                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect(value.getId() + " 红色警报!!!");
                            throw new RuntimeException("自己抛的");
                        }
                        state.update(value.getVc());
                    }
                })
                //创造一个kafkasink
                .addSink(new FlinkKafkaProducer<String>("sinksensor",
                        new KafkaSerializationSchema<String>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                                return new ProducerRecord<>("kafkasink",element.getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        sinkProps,
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                        ));
        env.execute();
    }
}
