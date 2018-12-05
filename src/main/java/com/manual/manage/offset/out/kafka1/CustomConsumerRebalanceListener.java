package com.manual.manage.offset.out.kafka1;

import com.manual.DataProcess;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 第一次加入消费者组的时候，rebalance会执行，也就是启动本地consumer的时候，当有其他消费者加入的时候也会调用。
 * <p>
 * <p>
 * kafka不同topic的consumer如果用的groupid名字一样的情况下，其中任意一个topic的consumer重新上下线都会造成剩余所有的consumer产生reblance行为
 * <p>
 * ，即使大家不是同一个topic，这主要是由于kafka官方支持一个consumer同时消费多个topic的情况，所以在zk上一个consumer出问题后zk是直接把group下面所有的consumer都通知一遍，这个与以前观念里认为group从属于某一个topic的概念完全不同，非常坑。
 *
 * @param <K>
 * @param <V>
 */
public class CustomConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {


    private KafkaConsumer<K, V> kafkaConsumer;
    private List<ConsumerRecord<K, V>> buffer;
    private String consumerGroupId;
    private int errorCount_perBatch;  //记录每批次错误尝试次数

    private DataProcess dataProcess;

    public CustomConsumerRebalanceListener(KafkaConsumer<K, V> kafkaConsumer, List<ConsumerRecord<K, V>> buffer, String consumerGroupId, int errorCount_perBatch) {
        this.kafkaConsumer = kafkaConsumer;
        this.buffer = buffer;
        this.consumerGroupId = consumerGroupId;
        this.errorCount_perBatch = errorCount_perBatch;
    }


    {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        this.dataProcess = context.getBean(DataProcess.class);
    }


    /**
     * 在调用poll时，poll方法被阻塞，先执行所以消费者的onPartitionRevoked方法，再进行rabanlace，之后所以消费者在执行各自的onPartitionsAssigned，最后在恢复继续执行 poll
     * It is guaranteed that all the processes in a consumer group will execute their onPartitionsRevoked(Collection) callback before any instance executes its onPartitionsAssigned(Collection) callback.
     * <p>
     * onPartitionRevoked会在rebalance操作之前调用，用于我们提交本地消费者偏移到数据库
     * <p>
     * Another common use for onPartitionRevoked is to flush any caches the application maintains for current partitions .
     * <p>
     * A callback method the user can implement to provide handling of offset commits to a customized store on the start of a rebalance operation.
     * This method will be called before a rebalance operation starts and after the consumer stops fetching data.
     * It is recommended that offsets should be committed in this callback to either Kafka or a custom offset store to prevent duplicate data.
     *
     * @param partitions rebalance前，当前consumer 被分配消费的所有partition.
     *                   The list of partitions that were assigned to the consumer on the last rebalance .
     */
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        System.out.println("onPartitionsRevoked begin ");
        if (partitions.size() != 0) {
            if (buffer.size() != 0) {
                try {
                    dataProcess.processMessExactOnce(buffer, kafkaConsumer, consumerGroupId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                buffer.clear();
            }
            errorCount_perBatch = 0;
        }

        System.out.println("onPartitionsRevoked end ");
    }


    /**
     * onPartitionAssigned会在rebalance操作之后调用，用于我们拉取数据库新的分配区的偏移量同步到本地。
     * the consumer will want to look up the offset for those new partitions and correctly initialize the consumer to that position 。
     * <p>
     * A callback method the user can implement to provide handling of customized offsets on completion of a successful partition re-assignment.
     * This method will be called after the partition re-assignment completes and before the consumer starts fetching data, and only as the result of a poll(long) call.
     *
     * @param partitions rebalance前，当前consumer 最新被分配消费的所有partition .
     *                   The list of partitions that are now assigned to the consumer (may include partitions previously assigned to the consumer) .
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionsAssigned begin ");
//        System.out.println(partitions.size());
        for (TopicPartition partition : partitions) {
            long offset = dataProcess.findCommitedOffsetsFromDB(partition, consumerGroupId);
            System.out.println(partition.topic() + "---" + partition.partition() + "---" + offset);
            if (offset == 0) {
                kafkaConsumer.seekToBeginning(Arrays.asList(partition));
            } else {
                kafkaConsumer.seek(partition, offset);
            }
        }


        System.out.println("onPartitionsAssigned end ");
    }


}
