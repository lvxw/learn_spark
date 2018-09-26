package com.test.pattern
import org.apache.spark.SparkConf

class TestStrategy extends PatternStrategy {
  /**
    * 获取saprkConf对象
    *
    * @param appName
    * @return
    */
  override def getSparkConf(appName: String): SparkConf = {
    new SparkConf().setAppName(appName).setMaster("local")
  }
}
