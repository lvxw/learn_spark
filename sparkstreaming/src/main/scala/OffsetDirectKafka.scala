import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 建立topic:
  *   /home/hadoop/soft/kafka/bin/kafka-topics.sh --zookeeper artemis-02:2181/microlens/artemis/kafka --create --topic wordCount --partitions 3  --replication-factor 3
  * 向kafka 中输入数据：
  *   /home/hadoop/soft/kafka/bin/kafka-console-producer.sh --broker-list artemis-02:9092 --topic wordCount
  *
  */
object OffsetDirectKafka{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("OffsetDirectKafka")
      .setMaster("local[*]")
//      .set("spark.streaming.kafka.maxRatePartition","10000")

    val ssc = new StreamingContext(conf,Seconds(5))

    val group = "OffsetDirectKafka"
    val topic = "wordCount"
    val brokerList = "artemis-02:9092,artemis-03:9092,artemis-04:9092"
    val zkQuorum = "artemis-02:2181,artemis-03:2181,artemis-03:2181"
    val zkTopicPath = new ZKGroupTopicDirs(group,topic).consumerOffsetDir

    val zkClient = new ZkClient(zkQuorum)
    val children = zkClient.countChildren(zkTopicPath)
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )
    var kafkaStream:InputDStream[(String,String)] = null


    if(children > 1){
      for(i <- 0 until children){
        val partitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
        fromOffsets += (TopicAndPartition(topic,i) -> partitionOffset.toLong)
      }
      val messageHander = (mmd:MessageAndMetadata[String,String]) =>(mmd.topic,mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHander)
    }else{
      kafkaStream =  KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))
    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream
      .transform{ rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map(msg => msg._2)
      .foreachRDD{rdd =>
        rdd.foreachPartition { partition =>
          partition.foreach(println)
        }
        for(o <- offsetRanges){
          ZkUtils.updatePersistentPath(zkClient,s"${zkTopicPath}/${o.partition}",o.fromOffset.toString)
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }

}
