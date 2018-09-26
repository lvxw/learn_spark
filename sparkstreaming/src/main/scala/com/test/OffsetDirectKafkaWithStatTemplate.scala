package com.test

import com.test.pattern.BaseProgram
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.kafka.HasOffsetRanges


/**
  * 建立topic:
  *   /home/hadoop/soft/kafka/bin/kafka-topics.sh --zookeeper artemis-02:2181/microlens/artemis/kafka --create --topic wordCount --partitions 3  --replication-factor 3
  * 向kafka 中输入数据：
  *   /home/hadoop/soft/kafka/bin/kafka-console-producer.sh --broker-list artemis-02:9092 --topic wordCount
  *
  * 建立topic:
  *   /home/hadoop/soft/kafka/bin/kafka-topics.sh --zookeeper artemis-02:2181/microlens/artemis/kafka --create --topic wordCount2 --partitions 3  --replication-factor 3
  * 向kafka 中输入数据：
  *   /home/hadoop/soft/kafka/bin/kafka-console-producer.sh --broker-list artemis-02:9092 --topic wordCount2
  */

/**
  * 注意：如果中途动态添加topic,启动任务前请在zookeeper对象目录手动添加节点信息，否则不会加载
  */

/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"batch_duration\":\"5\",
          \"topics_partition\":\"wordCount|3,wordCount2|3\",
          \"broker_list\":\"artemis-02:9092,artemis-03:9092,artemis-04:9092\",
          \"zk_quorum\":\"artemis-02:2181,artemis-03:2181,artemis-04:2181\",
          \"checkpoint_dir\":\"tmp/streaming\",
          \"run_pattern\":\"TestStrategy\"
        }
  */
object OffsetDirectKafkaWithStatTemplate extends BaseProgram{
  initParams(args)
  getStreamingContext()
  sc.checkpoint(checkpointDir)

  val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.map(t => (t._1,t._2.sum+t._3.getOrElse(0)))
  }

  val re = getKafkaStream()
    .transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    .map(msg => msg._2)
    .flatMap(_.split(" "))
    .map((_,1))
    .updateStateByKey(newUpdateFunc,new HashPartitioner(sc.sparkContext.defaultParallelism),true)
    .foreachRDD{rdd =>
      rdd.foreach(println(_))
      updateTopicsOffset()
    }



  startStreamingContext()
}
