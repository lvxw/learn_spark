package com.test

import org.apache.spark.{SparkConf, SparkContext}

class SparkShell{


  /**
    * /usr/local/spark/bin/spark-shell --master yarn --deploy-mode client --conf spark.default.parallelism=240 --driver-memory 2G --executor-memory 4G --driver-cores 2 --num-executors  20  --executor-cores 4
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/dw/oxeye/log_dts/dts/task_stat/2018/10/23/*/")
    val rdd2 = rdd.filter(x => x.contains("funotc") && x.contains("mac="))
    val rdd3 = rdd2.map(x => (x.split("mac=")(1).split("&")(0),1))
    val rdd4 = rdd3.reduceByKey(_+_)
    rdd4.map(x =>x._1+","+x._2).saveAsTextFile("/tmp/lvxw/20181024/uvByMac")
  }


}



//rprotocol=1&dev=&mac=2876CD1216C3&ver=4.1.1.20&nt=0&fudid=0000F3A700A81200FB222876CD1210180A9D16C3&kver=5.0.0.131&value=2|106.85.8.60|2|3|0|0|0|0|tv_ad&mr_ip=106.85.8.60&mr_time=23/Oct/2018:00:00:00 +0800&mr_useragent=-

