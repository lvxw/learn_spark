package com.test.pattern

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD;

/**
  *  抽象策略
  */
trait PatternStrategy {

  /**
    * 获取Sparkcontext对象
    * @param appName  应用名称
    * @return
    */
  def getSparkContext(appName:String):SparkContext;

  /**
    * 从HDFS路径创建RDD
    * @param sc           Sparkcontext对象
    * @param inputDir     HDFS输入路径
    * @return
    */
  def getInitRDD(sc:SparkContext,inputDir:String):RDD[String];

}
