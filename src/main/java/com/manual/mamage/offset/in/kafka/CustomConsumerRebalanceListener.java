package com.manual.mamage.offset.in.kafka;

import com.manual.DataProcess;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;

/**
 * Hello world!
 */
public class CustomConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {


    private KafkaConsumer<K, V> kafkaConsumer;
    private List<ConsumerRecord<K, V>> buffer;

    private DataProcess dataProcess;

    public CustomConsumerRebalanceListener(KafkaConsumer<K, V> kafkaConsumer, List<ConsumerRecord<K, V>> buffer) {
        this.kafkaConsumer = kafkaConsumer;
        this.buffer = buffer;
    }


    {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        this.dataProcess = context.getBean(DataProcess.class);
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        System.out.println("onPartitionsRevoked begin ");
        if (partitions.size() != 0) {
            if (buffer.size() != 0) {
                for (TopicPartition partition : partitions) {
                    System.out.println(partition.topic() + "******" + partition.partition() + "******" + kafkaConsumer.position(partition));
                }
                try {
                    dataProcess.processMess(buffer);
                    kafkaConsumer.commitSync();
                }catch (Exception e){
                    e.printStackTrace();
                    //这里就不用再seek了，因为接下来就会执行onPartitionsAssigned ， offset 值从数据库或 kafka __consumer_offsets topic 中获取。
                }
                buffer.clear();

            }
        }

        System.out.println("onPartitionsRevoked end ");
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionsAssigned begin ");

        for (TopicPartition partition : partitions) {
            if (kafkaConsumer.committed(partition) == null) {
                System.out.println(partition.topic() + "---" + partition.partition()+ " >>>>>>>");
                kafkaConsumer.seekToBeginning(Arrays.asList(partition));
            } else {
                System.out.println(partition.topic() + "---" + partition.partition() + "---" + kafkaConsumer.committed(partition).offset());
                kafkaConsumer.seek(partition, kafkaConsumer.committed(partition).offset());
            }
        }
        System.out.println("onPartitionsAssigned end ");
    }



}
