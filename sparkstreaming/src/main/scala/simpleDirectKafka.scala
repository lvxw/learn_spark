import kafka.api.OffsetRequest
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 建立topic:
  *   /home/hadoop/soft/kafka/bin/kafka-topics.sh --zookeeper artemis-02:2181/microlens/artemis/kafka --create --topic wordCount --partitions 3  --replication-factor 3
  * 向kafka 中输入数据：
  *   /home/hadoop/soft/kafka/bin/kafka-console-producer.sh --broker-list artemis-02:9092 --topic wordCount
  *
  */
object simpleDirectKafka {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("simpleDirectKafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))


    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "artemis-02:9092,artemis-03:9092,artemis-04:9092",
      "group.id" -> "g1",
      "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )
    val topics = Set("wordCount")
    val directStream:InputDStream[(String,String)]= KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

    directStream.map(_._2)
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
