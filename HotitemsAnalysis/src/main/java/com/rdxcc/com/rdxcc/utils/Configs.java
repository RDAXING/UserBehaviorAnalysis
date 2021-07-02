package com.rdxcc.com.rdxcc.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * 项目：UserBehaviorAnalysis
 * 包名：com.rdxcc.com.rdxcc.utils
 * 作者：rdx
 * 日期：2021/7/2 15:10
 */
public class Configs {
    public static Properties getKafkaConfig(String groupid){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"cdhcm.fahaicc.com:9092,cdh1.fahaicc.com:9092,cdh2.fahaicc.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        return  properties;
    }
}
