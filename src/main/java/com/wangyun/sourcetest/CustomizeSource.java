package com.wangyun.sourcetest;


/**
 * @Author Missouri
 * @Date 2021-7-16
 */

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;


public class CustomizeSource {

}
class mySource implements SourceFunction {


    @Override
    public void run(SourceContext ctx) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop162:9092,hadoop163:9092,hadoop164:9092");
        properties.setProperty("group.id","kafkatoflink");
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);


    }

    @Override
    public void cancel() {

    }
}
