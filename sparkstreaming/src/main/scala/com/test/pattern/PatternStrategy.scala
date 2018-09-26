package com.test.pattern

import org.apache.spark.SparkConf;

/**
  *  抽象策略
  */
trait PatternStrategy {

  /**
    * 获取saprkConf对象
    * @param appName
    * @return
    */
  def getSparkConf(appName:String):SparkConf;

}
