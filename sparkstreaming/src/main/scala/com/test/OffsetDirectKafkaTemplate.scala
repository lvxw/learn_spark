package com.test

import com.test.pattern.BaseProgram
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
          \"run_pattern\":\"TestStrategy\"
        }
  */
object OffsetDirectKafkaTemplate extends BaseProgram{
  initParams(args)
  getStreamingContext()

  def test1(): Unit ={
    getKafkaStream()
      .transform{ rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map(msg => msg._2)
      .foreachRDD{rdd =>
        rdd
          .flatMap(x => x.split(" "))
          .map((_,1))
          .reduceByKey(_+_)
          .foreachPartition(partition =>
            // 此处可以是外部数据库链接，一个分区使用一个链接
            partition.foreach(println(_))
          )
        updateTopicsOffset()
      }
  }

  def test2(): Unit ={
    getKafkaStream()
    .transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    .map(msg => msg._2)
    .foreachRDD{rdd =>
      val re = rdd
        .flatMap(x => x.split(" "))
        .map((_,1))
        .reduceByKey(_+_)

      re.foreach(println(_))
      updateTopicsOffset()
    }
  }

  test1()
  startStreamingContext()
}
