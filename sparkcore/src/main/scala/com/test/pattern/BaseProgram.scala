package com.test.pattern

import org.apache.spark.SparkContext

import scala.io.Source

/**
  * author:         吕学文<lvxw@fun.tv>
  * date:           2018-03-28
  * Description:    spark程序的父类
  * jira:
  */
class BaseProgram{
  var inputDir:String = _
  var outputDir:String = _
  var interimDir:String = _
  var dataDate:String = _
  var runPattern:String = _
  var DELIMITER = "\t"
  val appName = this.getClass.getSimpleName.replace("$", "")
  val STRATEGY_PACKAGE = "com.test.pattern."


  var sc:SparkContext = _
  var context:StrategyContext = _

  def initSparkContext(): Unit ={

    val strategy = Class.forName(STRATEGY_PACKAGE+runPattern).newInstance().asInstanceOf[PatternStrategy]
    context = new StrategyContext(strategy)
    sc = context.executeGetSparkContext(appName)
  }

  /**
    * 通过模式匹配解析json类型参数
    * @param json
    * @return
    */
  def matchJson(json:Option[Any]) =json match {
    case Some(map: Map[String, Any]) => map
  }



  /**
    * 初始化参数inputDir，outputDir
    * @param json
    * @return
    */
  def initParams(json:Option[Any]):Map[String,Any] = {
    val paramsMap = matchJson(json)
    inputDir = paramsMap.get("input_dir").getOrElse().toString
    outputDir = paramsMap.get("output_dir").getOrElse().toString
    interimDir = paramsMap.get("interim_dir").getOrElse().toString
    dataDate = paramsMap.get("data_date").getOrElse().toString
    runPattern = paramsMap.get("run_pattern").getOrElse().toString
    paramsMap
  }

  /**
    * 读取classpath下的配置文件，保存在list集合中
    * @param path
    * @return
    */
  def readFileToList(path:String): List[String] ={
    Source.fromURL(this.getClass.getClassLoader.getResource(path)).getLines().toList
  }
}