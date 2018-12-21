package cs6240.hw2

import java.nio.file.Paths

object ReduceByKey {

  def main(args: Array[String]) {

    Utils.run(sc => {
      val edges = sc.textFile(args(0)).map(line => (line.split(",")(0), 1))

      val c3 = edges.reduceByKey(_ + _)

      println("reduceByKey:", c3.toDebugString)

      c3.saveAsTextFile(Paths.get(args(1), "reduceByKey").toString)

    }, "reduceByKey")
  }

}