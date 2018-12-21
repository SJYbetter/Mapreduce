package cs6240.hw1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object twitter_follwers {

  def main(args: Array[String]) {

    //reduceByKeys()

    process_followers(args)
  }

  def process_followers(args: Array[String]) {

    val conf = new SparkConf().setAppName("twitter_followers")
    val sc = new SparkContext(conf)

    val nodes = sc.textFile(args(0)).map(word => (word, 0))
    val edges = sc.textFile(args(1)).map(line => (line.split(",")(0),1))

//    nodes.foreach(println)
//    edges.foreach(println)

    val counters = nodes.union(edges).reduceByKey(_ + _)

    counters.saveAsTextFile(args(2))
    //counters.foreach(println)
    sc.stop()
  }

  def reduceByKeys() {
    val conf = new SparkConf().setAppName("MyTestApp").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val x = sc.parallelize(List("a", "b", "a", "a", "b", "b", "b", "b"))
    val s = x.map((_, 1))
    val result = s.reduceByKey((pre, after) => pre + after)

    result.foreach(println)
  }
}