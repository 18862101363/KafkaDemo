package com;

import com.manual.DataProcess;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by dong_zhengdong on 2018/12/3.
 */

@ContextConfiguration(locations = {"classpath:applicationContext.xml"})
public class APPTest extends AbstractJUnit4SpringContextTests {

    @Autowired
    private DataProcess dataProcess;

    @Test
    public void testFindCommitedOffsetsFromDB() {


        long commitedOffsetsFromDB = dataProcess.findCommitedOffsetsFromDB(new TopicPartition("topic3", 0), "group1");
        System.out.println(commitedOffsetsFromDB);
    }


}
