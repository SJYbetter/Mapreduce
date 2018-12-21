package cs6240.hw2

import java.nio.file.Paths

object AggregateByKey {

  def main(args: Array[String]) {

    Utils.run(sc => {
      val edges = sc.textFile(args(0)).map(line => (line.split(",")(0), 1))

      // http://blog.cheyo.net/157.html
      val c2 = edges.aggregateByKey(0)(_ + _, _ + _)
      println("aggregateByKey:", c2.toDebugString)

      c2.saveAsTextFile(Paths.get(args(1), "aggregateByKey").toString)

    }, "aggregateByKey")

  }
}