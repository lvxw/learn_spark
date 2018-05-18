package com.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Record(key: Int, value: String)

object TestRddToSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
    df.registerTempTable("records")

    println("Result of SELECT *:")
    val rdd = sqlContext.sql("SELECT * FROM records").rdd.map(x => x(0)+","+x(1))
    rdd.saveAsTextFile("E:\\project_sync_repository\\learn_spark\\sparksql\\data\\ss")

  }

}
