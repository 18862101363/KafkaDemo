package com.manual.manage.offset.out.kafka2;

import com.manual.DataProcess;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.support.ClassPathXmlApplicationContext;


import java.util.*;

/**
 * https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html    API
 *
 *
 * offset保存在kafka的内部 __consumer_offsets ， 处理方式与下面类似。
 *
 *
 * 目前我个人觉得下面处理方式最好， exact-once
 */
public class MyKafkaConsumer implements Runnable {

    private String consumerGroupId;
    private String[] topic;
    private int minMsgsPerBatch = 3;      // 每批次最少处理消息的数量
    private int errCheckTimesPerBatch = 3;  // 每批次错误检查次数
    private int maxErrBatchsTolerance = 2;    //每个消费者最大容忍的错误批次数量
    private Properties props;
    private DataProcess dataProcess;
    private KafkaConsumer<String, String> consumer;


    private List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();  //用于暂存足够数量消息再进行这一批次消息的处理
    private int errorBatchCounts = 0;       // 当前消费者已经成功跳过处理的错误批次数

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

        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        dataProcess = context.getBean(DataProcess.class);

        props = new Properties();
        props.put("bootstrap.servers", "172.16.62.128:9092,172.16.62.129:9092,172.16.62.130:9092");
        props.put("group.id", consumerGroupId);
        props.put("enable.auto.commit", "false");//false
        props.put("auto.commit.interval.ms", "1000");
        props.put("max.poll.interval.ms", "20000");  // consumer每次poll调用的时间间隔，超过这个间隔，即使consumer还往kafka发送heartbeat存活着，这时也被认为没有能力进行下一次的poll动作，则consumer将停止发送心跳，然后发送LeaveGroup请求主动离组，从而引发coordinator开启新一轮rebalance。
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //org.apache.kafka.clients.consumer.KafkaConsumer API 的 commitSync 是将下一个Offset信息存储在kafka内部的 __consumer_offsets topic 中，相应的取出方法是 committed
        consumer = new KafkaConsumer<String, String>(props);
        CustomConsumerRebalanceListener<String, String> cusConsuRebListener = new CustomConsumerRebalanceListener<String, String>(consumer, buffer, consumerGroupId);
        consumer.subscribe(Arrays.asList(topic), cusConsuRebListener);
        //下面assign 方式，不受rebalance影响
//        consumer.assign(Arrays.asList(new TopicPartition(topic,0),new TopicPartition(topic,1)));

    }

    @Override
    public void run() {

        String consumerName = Thread.currentThread().getName();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(consumerName + ":    ----topic: " + record.topic() + "----partition: " + record.partition()  + "----offset: " + record.offset()+ "----value: " + record.value());
                buffer.add(record);
            }

            if (buffer.size() >= minMsgsPerBatch) {
                dealSuccessllyOrFakeSuccesslly(0);
                System.out.println("buffer size: "+buffer.size());
            }
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


    /**
     *
     * 消息和offset都处理成功
     * 或者
     * 跳过该错误批次消息，单单更新offset
     *
     * @param errorCount_currentBatch    当前批次消息已经连续处理出错次数
     */
    public void dealSuccessllyOrFakeSuccesslly(Integer errorCount_currentBatch){

        try {
            //下面将消息的处理和 消息的下一个offset 放在一个 oracle 事务中，这样就 exact once
            dataProcess.processMessExactOnce(buffer, consumer, consumerGroupId);
            buffer.clear();
        } catch (Exception e) {
            e.printStackTrace();

            errorCount_currentBatch++;

            if (errorCount_currentBatch < errCheckTimesPerBatch) {
                // 错误尝试次数没有达到3次，继续尝试。
                dealSuccessllyOrFakeSuccesslly(errorCount_currentBatch);
            }else if (errorCount_currentBatch == errCheckTimesPerBatch) {
                // 不再尝试，开始下面kafka 信息处理

                try {

                    //单独保存 offsets 信息，以达到跳过此错误批次
                    dataProcess.justCommitOffsetsOnly(consumer, consumerGroupId);
                    //将错误批次的数据和offset邮件出去
                    dataProcess.emailErrorInfosOPerBatch(buffer, consumerGroupId, new Exception[]{e});
                    buffer.clear();

                    //错误批次数超出最大指定次数，就直接停止当前消费应用
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

            }

        }



    }


}
