package com.test.pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 具体策略类——IDE运行时，使用该策略
  */
class LocalPatternStrategy extends PatternStrategy {

  def getSparkContext(appName:String):SparkContext={
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.set("spark.hadoop.validateOutputSpecs","false")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")
    new SparkContext(conf)
  }

  def getInitRDD(sc:SparkContext,inputDir:String):RDD[String]={
    sc.textFile(inputDir)
  }

}
