package com.manual.manage.offset.out.kafka1;

import com.manual.DataProcess;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;

/**
 * https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html    API
 */
public class MyKafkaConsumer implements Runnable {

    private String consumerGroupId;
    private String[] topic;
    private int minMsgsPerBatch = 3;      // 每批次最少处理消息的数量
    private int errCheckTimesPerBatch = 3;  // 每批次错误检查次数
    private int maxErrBatchsTolerance = 2;    //每个消费者最大容忍的错误批次数量
    private Properties props;
    private DataProcess dataProcess;

    public MyKafkaConsumer(String consumerGroupId, String[] topic) {
        this.consumerGroupId = consumerGroupId;
        this.topic = topic;
        init();
    }

    public MyKafkaConsumer(String consumerGroupId, String[] topic,  int minMsgsPerBatch,int errCheckTimesPerBatch, int maxErrBatchsTolerance) {
        this(consumerGroupId, topic);
        this.minMsgsPerBatch = minMsgsPerBatch;
        this.errCheckTimesPerBatch = errCheckTimesPerBatch;
        this.maxErrBatchsTolerance = maxErrBatchsTolerance;
    }


    public void init(){
        props = new Properties();
        props.put("bootstrap.servers", "172.16.62.128:9092,172.16.62.129:9092,172.16.62.130:9092");
        props.put("group.id", consumerGroupId);
        props.put("enable.auto.commit", "false");//false
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.poll.interval.ms", "20000");  // consumer每次poll调用的时间间隔，超过这个间隔，即使consumer还往kafka发送heartbeat存活着，这时也被认为没有能力进行下一次的poll动作，则consumer将停止发送心跳，然后发送LeaveGroup请求主动离组，从而引发coordinator开启新一轮rebalance。
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        dataProcess = context.getBean(DataProcess.class);

    }

    @Override
    public void run() {

        //org.apache.kafka.clients.consumer.KafkaConsumer API 的 commitSync 是将下一个Offset信息存储在kafka内部的 __consumer_offsets topic 中，相应的取出方法是 committed
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        Integer errorCount_currentBatch = 0;    // 当前批次消息已经出错次数
        int errorBatchCounts = 0;       // 当前消费者已经成功跳过处理的错误批次数
        CustomConsumerRebalanceListener<String, String> cusConsuRebListener = new CustomConsumerRebalanceListener<String, String>(consumer, buffer, consumerGroupId, errorCount_currentBatch);
        consumer.subscribe(Arrays.asList(topic), cusConsuRebListener);
        //下面assign 方式，不受rebalance影响
//        consumer.assign(Arrays.asList(new TopicPartition(topic,0),new TopicPartition(topic,1)));


        String consumerName = Thread.currentThread().getName();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(consumerName + ":    ----topic: " + record.topic() + "----partition: " + record.partition()  + "----offset: " + record.offset()+ "----value: " + record.value());
                buffer.add(record);
            }

            if (buffer.size() >= minMsgsPerBatch) {
                try {
                    //下面将消息的处理和 消息的下一个offset 放在一个 oracle 事务中，这样就 exact once
                    dataProcess.processMessExactOnce(buffer, consumer, consumerGroupId);
                    errorCount_currentBatch = 0;
                } catch (Exception e) {
                    e.printStackTrace();

                    errorCount_currentBatch++;
                    if (errorCount_currentBatch == errCheckTimesPerBatch) {
                        // 不再尝试，开始下面kafka 信息处理
                        errorCount_currentBatch = 0;


                        try {
                            //这里不用再做徐像下面的seek到之前的动作，直接跳过这批error信息
                            //单独保存 offsets 信息，以达到跳过此错误批次
                            dataProcess.justCommitOffsetsOnly(consumer, consumerGroupId);
                            //将错误批次的数据和offset邮件出去
                            dataProcess.emailErrorInfosOPerBatch(buffer, consumerGroupId, new Exception[]{e});

                            //错误批次超出 指定次数，就直接停止当前消费应用
                            errorBatchCounts++;
                            if (errorBatchCounts == maxErrBatchsTolerance) {
                                String msg = consumerGroupId + "已经有处理到"+maxErrBatchsTolerance+"批次出错消息，现在已将消息消费处理关闭";
                                dataProcess.emailShutdownConsume(msg);
                                consumer.close();
                                System.exit(1);
                            }
                        } catch (Exception e1) {
                            e1.printStackTrace();

                            // 连单独保存 offsets 都出错，当前consumer就直接停止消费分配的 partition. 可以将当前消费直接关闭，等数据库连接解决了，再重新开启消费，到时该error批次就可以被跳过了。
//                            consumer.pause(consumer.assignment());
                            String msg = consumerGroupId + "offsets单独保存都出错了，现在已将消息消费处理关闭";
                            dataProcess.emailShutdownConsumeWithException(msg, new Exception[]{e, e1});
                            consumer.close();
                            System.exit(1);
                        }

                    } else {
                        // 错误尝试次数没有达到3次，继续尝试。
                        //如果是自己将offset存储在oracle，那么应该从数据库中获取到保存的最新offset
                        Set<TopicPartition> partitions = consumer.assignment();
                        for (TopicPartition partition : partitions) {
                            long offset = dataProcess.findCommitedOffsetsFromDB(partition, consumerGroupId);
                            if (offset == 0) {
                                consumer.seekToBeginning(Arrays.asList(partition));
                            } else {
                                consumer.seek(partition, offset);
                            }
                        }
                    }

                }


                buffer.clear();
            }
            try {
//                System.out.println("going to sleep ....");
                Thread.sleep(6000);
//                System.out.println("going to poll ....");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
