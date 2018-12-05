package com.sixth.group07.day08.kafkademo

import kafka.producer.Partitioner

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:57 2018/8/1
  * @ 
  */
class ConsumePartitioner extends Partitioner{
  override def partition(key: Any, numPartitions: Int): Int = {
    key.hashCode() % numPartitions
  }
}
