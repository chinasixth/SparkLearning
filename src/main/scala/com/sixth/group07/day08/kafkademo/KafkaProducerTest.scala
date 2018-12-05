package com.sixth.group07.day08.kafkademo

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:08 2018/8/1
  * @ 模拟一个生产者，把生产的数据发送到kafka集群的某个topic中
  * 实现自定义分区器
  */
object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    // 首先定义一个topic，可以写成参数的形式，从args中传入
    val topic = "test"

    // 创建配置信息类
    val props = new Properties()
    // 指定序列化类型
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    // 指定kafka集群
    props.put("metadata.broker.list",
      "192.168.216.115:9092,192.168.216.117:9092,192.168.216.118:9092")
    // 设置发送数据后的响应方式:0,1,-1
    // 0: producer不等待broker发送的ack
    // 1: 当leader接收到消息后发送ack
    // -1: 当所有的follower都同步消息成功后发送ack
    props.put("request.required.acks", "1")
    // 调用默认分区器
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    // 调用自定义分区器
    //    props.put("partitioner.class", "com.qf.gp1707.day08.ConsumePartitioner")

    // producer的配置类
    val config = new ProducerConfig(props)

    // 实例化Producer，第一个是偏移量，第二个是实际的值
    val producer: Producer[String, String] = new Producer(config)

    // 模拟生产一些数据
    for (i <- 1 to 10000) {
      val msg = s"$i: producer send data"
      producer.send(new KeyedMessage[String, String](topic, msg))
    }
  }
}
