package com.test.pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 具体策略类——测试集群测试时，使用该策略
  */
class ClusterTextPatternStrategy extends PatternStrategy {

  def getSparkContext(appName:String):SparkContext={
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }

  def getInitRDD(sc:SparkContext,inputDir:String):RDD[String]={
    sc.textFile(inputDir)
  }

}
