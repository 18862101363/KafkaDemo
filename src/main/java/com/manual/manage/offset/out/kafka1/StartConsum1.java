package com.manual.manage.offset.out.kafka1;

/**
 * Created by dong_zhengdong on 2018/12/5.
 */
public class StartConsum1 {


    public static void main(String[] args) {
        String consumerGroupId = "group1";
        String[] topics = new String[]{"topic1"};

        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(consumerGroupId, topics);
        new Thread(myKafkaConsumer).start();
    }


}
