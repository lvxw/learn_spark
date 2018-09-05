import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * nc -lk 888
  * 使用该命令发送数据
  */
object WordCountByTcpWithStat {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountByTcp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint("tmp/outs/streaming")

    val stream = ssc.socketTextStream("localhost",8888)

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.map(t => (t._1,t._2.sum+t._3.getOrElse(0)))
    }

    val result = stream
      .flatMap(_.split(" "))
      .map((_,1))
      .updateStateByKey(newUpdateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
