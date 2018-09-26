package com.test.pattern

import com.test.util.StringUtils
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

class BaseProgram extends App {
  var batchDuration:Long = _
  var checkpointDir:String = _
  var runPattern:String = _
  var appName:String = _
  val STRATEGY_PACKAGE = "com.test.pattern."
  var sc:StreamingContext = _

  var groupName:String = _
  var topicAndPartitionMap:Map[String,Int] = _
  var brokerList:String = _
  var zkQuorum:String = _

  var zkClient:ZkClient = _
  var topicAndPartitionPathMap:Map[String,String] = Map()
  var offsetRanges = Array[OffsetRange]()

  def initParams(args:Array[String]):Map[String,Any] = {
    val paramsMap = StringUtils.jsonStrToMap(args)
    appName = this.getClass.getSimpleName.replace("$", "")
    batchDuration = paramsMap.get("batch_duration").getOrElse().toString.trim.toLong
    checkpointDir = paramsMap.get("checkpoint_dir").getOrElse().toString
    runPattern = paramsMap.get("run_pattern").getOrElse().toString
    groupName = appName
    topicAndPartitionMap = paramsMap.get("topics_partition").getOrElse().toString.split(",").map{x =>
      val kv = x.split("\\|")
      (kv(0),kv(1).toInt)
    }.toMap
    brokerList = paramsMap.get("broker_list").getOrElse().toString
    zkQuorum = paramsMap.get("zk_quorum").getOrElse().toString
    zkClient = new ZkClient(zkQuorum)

    paramsMap
  }

  def getStreamingContext():Unit ={
    val strategy = Class.forName(STRATEGY_PACKAGE + runPattern).newInstance().asInstanceOf[PatternStrategy]
    sc = new StrategyContext(strategy).getStreamingContext(appName,batchDuration=batchDuration)
  }


  def getKafkaStream(): InputDStream[(String,String)]  ={
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupName,
      "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )

    val topicList = topicAndPartitionMap.keySet.toList
    val topicOffsetZkPathList = topicList.map(topic => new ZKGroupTopicDirs(groupName,topic).consumerOffsetDir)
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    for(i <- 0 until  topicOffsetZkPathList.size){
      val childrenNum = zkClient.countChildren(topicOffsetZkPathList(i))
      if(childrenNum>0){
        for(j <- 0 until childrenNum){
          val zkTopicPath = s"${topicOffsetZkPathList(i)}/${j}"
          val partitionOffset = zkClient.readData[String](zkTopicPath)
          fromOffsets += (TopicAndPartition(topicList(i),j) -> partitionOffset.toLong)
          topicAndPartitionPathMap += (topicList(i)+"_"+j -> zkTopicPath)
        }
      }else{
        val partitionNum = topicAndPartitionMap.getOrElse(topicList(i),3)
        for(j <- 0 until partitionNum){
          val zkTopicPath = s"${topicOffsetZkPathList(i)}/${j}"

          ZkUtils.updatePersistentPath(zkClient,zkTopicPath,"0")
          topicAndPartitionPathMap += (topicList(i)+"_"+j -> zkTopicPath)
        }

      }
    }

    if(fromOffsets.keySet.size > 0){
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](sc,kafkaParams,fromOffsets,(mmd:MessageAndMetadata[String,String]) =>(mmd.topic,mmd.message()))
    }else{
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](sc,kafkaParams,topicList.toSet)
    }
  }

  def updateTopicsOffset(): Unit ={
    for(o <- offsetRanges){
      val key = s"${o.topic}_${o.partition}"
      val path = topicAndPartitionPathMap.get(key).getOrElse().toString
      val offset = o.fromOffset
      ZkUtils.updatePersistentPath(zkClient,path,offset.toString)
    }
  }

  def startStreamingContext(): Unit ={
    sc.start()
    sc.awaitTermination()
  }
}
