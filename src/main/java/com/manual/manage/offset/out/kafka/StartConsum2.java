package com.manual.manage.offset.out.kafka;

/**
 * Created by dong_zhengdong on 2018/12/5.
 */
public class StartConsum2 {


    public static void main(String[] args) {
        String consumerGroupId = "group1";
        String topic = "topic4";

        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(consumerGroupId, new String[]{topic});
        new Thread(myKafkaConsumer).start();
    }


}
