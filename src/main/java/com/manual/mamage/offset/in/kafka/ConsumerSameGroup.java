package com.manual.mamage.offset.in.kafka;

import com.manual.DataProcess;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;

/**
 * Created by dong_zhengdong on 2018/11/30.
 */
public class ConsumerSameGroup {


    public static void main(String[] args) throws InterruptedException {

        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        DataProcess dataProcess = context.getBean(DataProcess.class);


        String TOPIC = "topic1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.62.128:9092,172.16.62.129:9092,172.16.62.130:9092");
        props.put("group.id", "group1");
        props.put("enable.auto.commit", "false");//false
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //org.apache.kafka.clients.consumer.KafkaConsumer API 的 commitSync 是将下一个Offset信息存储在kafka内部的 __consumer_offsets topic 中，相应的取出方法是 committed
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        CustomConsumerRebalanceListener<String, String> cusConsuRebListener = new CustomConsumerRebalanceListener<String, String>(consumer, buffer);
        consumer.subscribe(Arrays.asList(TOPIC), cusConsuRebListener);

        //下面assign 方式，不受rebalance影响
//        consumer.assign(Arrays.asList(new TopicPartition(TOPIC,0),new TopicPartition(TOPIC,1)));

        final int minBatchSize = 3;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("consumer1: " + record.topic() + "----" + record.partition() + "----" + record.value());
                buffer.add(record);
            }

            if (buffer.size() >= minBatchSize) {
                try {
                    //下面将获取的数据保存，再将offset保存到kafka __consumer_offsets topic 中
                    //这种方式，没法在同一个事务中，所以可能会出现 at_least_once 重复消费现象
                    dataProcess.processMess(buffer);
                    consumer.commitSync();

                } catch (Exception e) {
                    e.printStackTrace();
                    Set<TopicPartition> partitions = consumer.assignment();
                    for (TopicPartition partition : partitions) {
                        if (consumer.committed(partition) == null) {
                            consumer.seekToBeginning(Arrays.asList(partition));
                        } else {
                            consumer.seek(partition, consumer.committed(partition).offset());

                        }
                    }

                }
                buffer.clear();
            }
            Thread.sleep(6000);
        }


    }


}
