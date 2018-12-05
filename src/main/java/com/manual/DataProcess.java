package com.manual;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by dong_zhengdong on 2018/12/3.
 */
@Repository
public class DataProcess {

    @Autowired
    private SessionFactory sessionFactory;


    /**
     * @param mess
     * @param <K>
     * @param <V>
     */
    @Transactional
    public <K, V> void processMess(List<ConsumerRecord<K, V>> mess) {

        for (ConsumerRecord mes : mess) {  // 为代码，模拟将获取到的数据存入数据库
            System.out.println(mes.topic() + ":::::" + mes.partition() + ":::::" + mes.offset() + ":::::" + mes.value());
        }
        int num = 10 / 0;


    }


    /**
     *
     *  create table KAFKA_OFFSETS (
     CONSUMER_GROUP_ID VARCHAR2(50),
     TOPIC VARCHAR2(50),
     PARTITION NUMBER(15),
     OFFSET NUMBER(15)
     );
     *
     *
     */


    /**
     * 下面将消息的处理和消息的下一个offset放在一个 db 事务中，这样就 exact once
     *
     * @param mess
     * @param consumer
     * @param consumerGroupId
     * @param <K>
     * @param <V>
     */
    @Transactional
    public <K, V> void processMessExactOnce(List<ConsumerRecord<K, V>> mess, KafkaConsumer consumer, String consumerGroupId) {
        for (ConsumerRecord mes : mess) {  // 为代码，模拟将获取到的数据存入数据库
            System.out.println((mes.topic() + ":::::" + mes.partition() + ":::::" + mes.offset() + ":::::" + mes.value()));
            if ("tom".equals(mes.value())) {
                int num = 10 / 0;
            }
        }

        justCommitOffsetsOnly(consumer, consumerGroupId);
    }


    /**
     * 单独保存 offsets 信息，以达到跳过错误批次
     *
     * @param consumer
     * @param consumerGroupId
     * @param <K>
     * @param <V>
     */
    @Transactional
    public <K, V> void justCommitOffsetsOnly(KafkaConsumer consumer, String consumerGroupId) {


        Session session = sessionFactory.getCurrentSession();
        Set<TopicPartition> partitions = consumer.assignment();
        for (TopicPartition partition : partitions) {
            System.out.println(partition.topic() + ":::::" + partition.partition() + ":::::" + consumer.position(partition));
            List<BigDecimal> offsets = findCommitedOffsetsByPartitionAndconsumerGroupId(partition, consumerGroupId);
            String sql = null;
            if (offsets.size() == 0) {
                sql = "INSERT INTO KAFKA_OFFSETS VALUES ('" + consumerGroupId + "' ,'" + partition.topic() + "', " + partition.partition() + ", " + consumer.position(partition) + ")";
            } else {
                sql = "UPDATE KAFKA_OFFSETS SET OFFSET = " + consumer.position(partition) + " WHERE CONSUMER_GROUP_ID ='" + consumerGroupId + "' AND TOPIC = '" + partition.topic() + "' AND PARTITION = " + partition.partition();
            }

            session.createSQLQuery(sql).executeUpdate();

        }

    }


    /**
     * 下面从数据库中获取commited offset
     *
     * @param partition
     * @param consumerGroupId
     * @return
     */
    @Transactional
    public long findCommitedOffsetsFromDB(TopicPartition partition, String consumerGroupId) {
        List<BigDecimal> offsets = findCommitedOffsetsByPartitionAndconsumerGroupId(partition, consumerGroupId);
        return offsets.size() == 0 ? 0 : offsets.get(0).longValue();

    }


    /**
     * 下面从数据库中获取commited offset
     *
     * @param partition
     * @param consumerGroupId
     * @return
     */
    @Transactional
    private List<BigDecimal> findCommitedOffsetsByPartitionAndconsumerGroupId(TopicPartition partition, String consumerGroupId) {
        Session ses = sessionFactory.getCurrentSession();
        String sql = "SELECT OFFSET FROM KAFKA_OFFSETS where CONSUMER_GROUP_ID ='" + consumerGroupId + "' AND TOPIC = '" + partition.topic() + "' and PARTITION = " + partition.partition();
//        System.out.println(sql);
        List<BigDecimal> list = ses.createSQLQuery(sql).list();
        return list;
    }


    /**
     * 将错误批次的数据和offset邮件出去 ， 以人工检测错误，并将修改后的正确数据重新放入到 kafka中
     *
     * @param list
     */
    public void emailErrorInfosOPerBatch(List<ConsumerRecord<String, String>> list, String consumerGroupId, Exception[] errors) {

        StringBuilder builder = new StringBuilder("error batch infos:\n\n\n\n");

        HashMap<String, String> recordInfos = new HashMap<>();
        for (ConsumerRecord<String, String> record : list) {
            String key = consumerGroupId + "_" + record.topic() + "_" + record.partition();
            String value = "<" + record.offset() + "," + record.value() + ">\n";
            if (recordInfos.containsKey(key)) {
                recordInfos.put(key, recordInfos.get(key) + value);
            } else {
                recordInfos.put(key, value);
            }
        }

        for (Map.Entry entry : recordInfos.entrySet()) {
            builder.append(entry.getKey() + ":\n\n" + entry.getValue() + "\n\n");
        }

        builder.append(stringlizeExcep(errors));
        System.out.println(builder.toString());

    }


    /**
     *
     * @param errors
     * @return
     */
    public String stringlizeExcep(Exception[] errors) {
        StringBuilder builder = new StringBuilder();
        for (Exception exception : errors) {
            StringWriter stringWriter = new StringWriter();
            exception.printStackTrace(new PrintWriter(stringWriter));
            builder.append(stringWriter.toString()+"\n\n\n\n");
        }

        return builder.toString();
    }


    /**
     *
     * @param msg
     */
    public void emailShutdownConsume(String msg) {
        System.out.println(msg);
    }


    /**
     *
     * @param msg
     * @param errors
     */
    public void emailShutdownConsumeWithException(String msg,Exception[] errors) {
        StringBuilder builder = new StringBuilder();
        builder.append(msg+"\n\n\n");
        builder.append(stringlizeExcep(errors));

        System.out.println(builder.toString());
    }

}
