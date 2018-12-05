package com.sixth.group07.day08.kafkacopy

import java.util.Properties

import kafka.consumer._
import kafka.message.MessageAndMetadata

import scala.actors.threadpool.{ExecutorService, Executors}
import scala.collection.mutable

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 23:34 2018/8/3
  * @ 
  */
class KafkaConsumerTC(val stream: KafkaStream[Array[Byte], Array[Byte]]) extends Runnable{
  override def run(): Unit = {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext()){
      val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
      val topic: String = data.topic
      val msg = new String(data.message())
      val offset: Long = data.offset
      val partition: Int = data.partition
    }
  }
}

object KafkaConsumerTC{
  def main(args: Array[String]): Unit = {
    /*
    * 需求：从kafka中消费数据
    * 实质：就是从kafka中指定topic中获取数据
    * 技术：同样是创建一个配置信息类，用来封装所有的配置信息，然后用这个配置信息类来创建Consume的配置类
    * 再使用Consumer的配置类来创建Consumer对象
    * 获取kafka中的数据需要通过Consumer对象来创建一个访问kafka中topic的流，返回值类型是Map类型的，
    * 获取流时，并未指定是获取哪个topic，所以返回类型Map的key是以后我们要指定的topic，value是一个List
    * 当我们使用流，获取指定topic的数据，返回的仍然是上面的list，List的key是offset，value是真实的数据
    * 这样就拿到数据了
    * */
    val topic  = "test"

    val props = new Properties()
    props.put("group.id","group01")
    props.put("zookeeper.connect","hadoop05:2181,hadoop07:2181,hadoop08:2181")
    props.put("auto.offset.reset","smallest")

    val config = new ConsumerConfig(props)
    val consumer: ConsumerConnector = Consumer.create(config)
    val topicMap = new mutable.HashMap[String, Int]()
    topicMap.put(topic, 1)

    val streams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] = consumer.createMessageStreams(topicMap)
    val stream: Option[List[KafkaStream[Array[Byte], Array[Byte]]]] = streams.get(topic)

    // 使用多个线程输出结果
    // 创建线程池
    val pool: ExecutorService = Executors.newFixedThreadPool(3)
    // 使用线程池处理逻辑，传入的参数是实现了runnable的对象，执行的run方法
    // 此时可以自定义一个业务类，继承runnable，并实现run方法。
    // 因为我们写了一个object，可以直接写一个对应的class，实现Runnable，
    for (i <- 0 until stream.size){
      pool.execute(new KafkaConsumerTC(stream.get(i)))
    }

  }
}
