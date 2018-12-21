package cs6240.hw2

import java.nio.file.Paths

//import org.apache.spark.implicits._

object combining {

  def main(args: Array[String]) {
    //rdd_ops(args)

    dataset_ops(args)
  }

  case class FollowLink(from: Long, to: Long)

  def dataset_ops(args: Array[String]) {


    Utils.run_sql(spark => {
      val ds = spark.read.option("head", false).csv(args(0))
      val r0 = ds.groupBy("_c0").count()
      r0.collect
      println("groupBy: ", r0.explain(true))

    }, "dataset groupBy")
  }

  def rdd_ops(args: Array[String]) {


    Utils.run(sc => {
      val edges = sc.textFile(args(0)).map(line => (line.split(",")(0), 1))

      // https://blog.csdn.net/zongzhiyuan/article/details/49965021
      val c0 = edges.groupByKey().map(t => (t._1, t._2.sum))

      // paritally applied function && Currying
      // https://blog.csdn.net/bluishglc/article/details/51042940
      val c1 = edges.foldByKey(0)((x, y) => x + y)

      // http://blog.cheyo.net/157.html
      val c2 = edges.aggregateByKey(0)(_ + _, _ + _)

      val c3 = edges.reduceByKey(_ + _)

      println("groupByKey:", c0.toDebugString)
      println("foldByKey:", c1.toDebugString)
      println("aggregateByKey:", c2.toDebugString)
      println("reduceByKey:", c3.toDebugString)

      c0.saveAsTextFile(Paths.get(args(1), "groupByKey").toString)
      c1.saveAsTextFile(Paths.get(args(1), "foldByKey").toString)
      c2.saveAsTextFile(Paths.get(args(1), "aggregateByKey").toString)
      c3.saveAsTextFile(Paths.get(args(1), "reduceByKey").toString)

    }, "all jobs")
  }
}