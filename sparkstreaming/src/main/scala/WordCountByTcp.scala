import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * nc -lk 888
  * 使用该命令发送数据
  */
object WordCountByTcp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountByTcp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val stream = ssc.socketTextStream("localhost",8888)

    val result = stream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
