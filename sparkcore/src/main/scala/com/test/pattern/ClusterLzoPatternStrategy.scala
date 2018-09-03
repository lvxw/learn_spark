package com.test.pattern

import com.hadoop.mapreduce.LzoTextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 具体策略类——公网集群运行时，使用该策略
  */
class ClusterLzoPatternStrategy extends PatternStrategy {

  def getSparkContext(appName:String):SparkContext={
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }

  def getInitRDD(sc:SparkContext,inputDir:String):RDD[String]={
    sc.newAPIHadoopFile(inputDir,  classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text]).map{value =>
      value._2.toString
    }
  }

}
