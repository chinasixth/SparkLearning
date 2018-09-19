package com.sixth.gp1707.day08.kafkacopy

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 23:14 2018/8/3
  * @ 
  */
object KafkaProducerTC {
  def main(args: Array[String]): Unit = {
    /*
    * 需求：模拟kafka集群中的producer，生产数据
    * 实质：就是向kafka中写入一批数据
    * 技术：想要访问kafka集群，要指定zookeeper的地址、topic、数据序列化类型、
    * 发送数据后的响应方式、设置分区器（默认分区器/自定义分区器）
    * 除了指定topic以外的其他参数放到Properties中，作为PropertiesConfig的参数，
    * 可以获得Producer的配置对象，然后通过这个配置对象可以创建Producer对象
    * */
    val topic = "test"

    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list",
      "192.168.216.115:9092,192.168.216.117:9092,192.168.216.118:9092")
    props.put("request.required.acks", "1")
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")

    val config = new ProducerConfig(props)

    // 这里要注意修改类型，要和send方法中参数的泛型一致
    val producer: Producer[String, String] = new Producer(config)

    producer.send(new KeyedMessage[String, String](topic, "hello girl"))
  }
}
