package com.test

import org.apache.spark.{SparkConf, SparkContext}

object TestApp extends App with MyTrait {
  val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
  val sc = new SparkContext(sparkConf)

  val rdd = sc.parallelize(List(1, 2, 3, 4))
  println(rdd.count())

  val re = rdd.mapPartitions{x =>
    x.map(_*2).toList.toIterator
  }

  private val ints: Array[Int] = re.collect()
  println(ints)

}
