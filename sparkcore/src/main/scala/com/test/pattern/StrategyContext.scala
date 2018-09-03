package com.test.pattern

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 环境角色,持有一个PatternStrategy的引用
  * @param strategy
  */
class StrategyContext(strategy:PatternStrategy) {

  def executeGetSparkContext(appName:String): SparkContext ={
    strategy.getSparkContext(appName)
  }

  def executeGetInitRDD(sc:SparkContext,inputDir:String):RDD[String] = {
    strategy.getInitRDD(sc,inputDir);
  }

}
