package com.test

import com.test.pattern.BaseProgram


/**
  *
  * @param args (IDE 本地测试时，参数为：
        {
          \"input_dir\":\"tmp/logs/test\",
          \"output_dir\":\"tmp/outs/test\",
          \"run_pattern\":\"LocalPatternStrategy\"
        }
  */
object WordCount extends BaseProgram {

  initParams(args)
  initSparkContext()

  context
    .executeGetInitRDD(sc, inputDir)
    .flatMap(_.split(","))
    .map((_, 1))
    .reduceByKey(_ + _)
    .saveAsTextFile(outputDir)
}
