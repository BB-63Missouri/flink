package com.wangyun.sourcetest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;
/**
 * @Author Missouri
 * @Date 2021-7-16
 */
//启动kafka之后producer 命令进入消费者模式
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id","kafkatoflink");
        //p1:生产者的主题名 ， 反序列化器， peoperties
        /*FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("p1", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();*/
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("p1", new SimpleStringSchema(), properties).setStartFromLatest());
        source.print();
        env.execute();
    }



 /*   public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("group.id", "Flink03_Source_Kafka");
        props.setProperty("auto.reset.offset", "latest");

       *//* DataStreamSource<String> source = env.addSource(
            new FlinkKafkaConsumer<String>(
                "s1",
                new SimpleStringSchema(),
                props
            )
        );*//*

        DataStreamSource<ObjectNode> source = env.addSource(
                new FlinkKafkaConsumer<ObjectNode>(
                        "s1",
                        new JSONKeyValueDeserializationSchema(true),
                        props
                )
        );

        source
                //.map(node -> node.get("value"))
                .print();

        env.execute();

    }*/
}
