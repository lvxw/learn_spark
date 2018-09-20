import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 建立topic:
  *   /home/hadoop/soft/kafka/bin/kafka-topics.sh --zookeeper artemis-02:2181/microlens/artemis/kafka --create --topic wordCount --partitions 3  --replication-factor 3
  * 向kafka 中输入数据：
  *   /home/hadoop/soft/kafka/bin/kafka-console-producer.sh --broker-list artemis-02:9092 --topic wordCount
  *
  */
object WordCountByKafka {

  def main(args: Array[String]): Unit = {
    //至少两个线程
    val conf = new SparkConf().setAppName("WordCountByTcpWithStat").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val zkQuorum = "artemis-02:2181,artemis-03:2181,artemis-04:2181/microlens/artemis/kafka"
    val topic = "wordCount"
    val groupId = "words"

    val result = KafkaUtils.createStream(ssc,zkQuorum,groupId,Map(topic->3))
      .map(_._2)
      .flatMap(_.split(" "))
      .map((_,1))

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
