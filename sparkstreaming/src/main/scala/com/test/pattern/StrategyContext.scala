package com.test.pattern

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 环境角色,持有一个PatternStrategy的引用
  * @param strategy
  */
class StrategyContext(strategy:PatternStrategy) {

  def getStreamingContext(appName:String,batchDuration:Long=5): StreamingContext ={
    val sparkConf = strategy.getSparkConf(appName)
    new StreamingContext(sparkConf,Seconds(batchDuration))
  }


}
