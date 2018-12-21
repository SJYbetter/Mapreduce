package cs6240.hw2

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Utils {

  def run(callback: (SparkContext) => Unit, name: String, waitTimeout: Int = 120000) {
    val conf = new SparkConf().setAppName(name)
    val sc = new SparkContext(conf)

    callback(sc)

    if (waitTimeout > 0)
      Thread.sleep(waitTimeout)

    sc.stop()
  }

  def run_sql(callback: (SparkSession) => Unit, name: String, waitTimeout: Int = 12000): Unit = {
    val spark = SparkSession.builder().appName(name)
      .getOrCreate()

    callback(spark)

    if (waitTimeout > 0)
      Thread.sleep(waitTimeout)

    spark.close()
  }
}
