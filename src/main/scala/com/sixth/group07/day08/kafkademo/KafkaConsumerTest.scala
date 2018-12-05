package com.sixth.group07.day08.kafkademo

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerIterator, KafkaStream}

import scala.actors.threadpool.Executors
import scala.collection.mutable

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:23 2018/8/1
  * @ 实现一个消费者来消费kafka集群中的数据
  */
class KafkaConsumerTest(val consumer: String,
                        val stream: KafkaStream[Array[Byte], Array[Byte]])
  extends Runnable {
  override def run(): Unit = {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext()) {
      // 整个topic
      val data = it.next()
      // topic
      val topic = data.topic
      // 分区数
      val partition = data.partition
      // 数据的偏移量
      val offset = data.offset
      // 数据
      val msg = new String(data.message)
//      val msg: String = data.message().toString

      println(s"consumer:$consumer, topic:$topic, " +
        s"partition:$partition, offset:$offset, msg:$msg")
    }
  }
}

object KafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    // 定义要获取的topic
    val topic = "test"

    // 定义一个map，用于存储多个topic
    val topics = new mutable.HashMap[String, Int]()
    topics.put(topic, 2)

    // 配置信息
    val props = new Properties()
    // consumer group，要给消费者起一个组名
    props.put("group.id", "group02")
    // 指定zookeeper的列表
    props.put("zookeeper.connect",
      "192.168.216.115:2181,192.168.216.117:2181,192.168.216.118:2181")
    // 指定offset值
    props.put("auto.offset.reset", "smallest")

    // 创建consumer信息类
    val config = new ConsumerConfig(props)

    // 实例化Consumer，有数据时，会一直消费数据，如果没有数据，会线程等待
    val consumer = Consumer.create(config)

    // 获取数据, Map中的key是指topic，List代表读取的每个topic的数据
    val streams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] =
      consumer.createMessageStreams(topics)

    // 获取指定topic的数据,List中key是指offset，value是指数据
    val stream: Option[List[KafkaStream[Array[Byte], Array[Byte]]]] =
      streams.get(topic)

    // 创建一个固定线程数量的线程池
    val pool = Executors.newFixedThreadPool(3)

    for (i <- 0 until stream.size) {
      pool.execute(new KafkaConsumerTest(s"Consumer$i", stream.get(i)))
    }
  }
}
