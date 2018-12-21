package cs6240.hw2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Utils {

  def run(name: String, callback: (SparkContext) => Unit, waitTimeout: Int = 120000) {
    val conf = new SparkConf().setAppName(name)
    val sc = new SparkContext(conf)

    callback(sc)

    if (waitTimeout > 0)
      Thread.sleep(waitTimeout)

    sc.stop()
  }

  def run_sql(name: String, callback: (SparkSession) => Unit, waitTimeout: Int = 12000): Unit = {
    val spark = SparkSession.builder().appName(name)
      .getOrCreate()

    callback(spark)

    if (waitTimeout > 0)
      Thread.sleep(waitTimeout)

    spark.close()
  }

  def read_rdd(path: String, sc: SparkContext): RDD[(Int, Int)] = {
    val edges = sc.textFile(path).map(line => {
      val y = line.split(",")
      (Integer.parseInt(y(0)), Integer.parseInt(y(1)))
    })

    return edges
  }
}
