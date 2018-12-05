package com.manual.manage.offset.out.kafka2;

/**
 * Created by dong_zhengdong on 2018/12/5.
 */
public class StartConsum1 {


    public static void main(String[] args) {
        String consumerGroupId = "group1";
        String[] topics = new String[]{"topic1"};

        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(consumerGroupId, topics);


        // 我是选择一个线程控制一个consumer
        new Thread(myKafkaConsumer).start();
        //
        // https://www.cnblogs.com/huxi2b/p/6124937.html Kafka Consumer多线程实例  ，
        // 链接介绍了类似我的单个线程控制单个consumer，当然，我是按批次处理的，这样可以管理好offset,
        // 另外，作者还介绍了另一种方式，即单个consumer对 poll的数据再使用线程池并发处理。
        // 我对作者的方案2不认同，因为线程池并发处理单个consumer一批次的数据，处理无序，数据的offset保存后，consumer挂后重启后，无法确定哪些offsets处理成功，哪些失败了，不知从哪个offset位置读起，造成数据处理丢失，虽然作者说不会。
        // 而且为了达到并发，我完全可以启多个线程，每个线程管理一个consumer，数据不丢失，exact-once
        // 我也可以使用线程池创建多个consumer，线程池中的每个线程单独管理一个consumer


    }


}
