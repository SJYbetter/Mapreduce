package cs6240.hw2

import java.nio.file.Paths

object GroupByKey {

  def main(args: Array[String]) {

    Utils.run(sc => {

      val edges = sc.textFile(args(0)).map(line => (line.split(",")(0), 1))

      // https://blog.csdn.net/zongzhiyuan/article/details/49965021
      val c0 = edges.groupByKey().map(t => (t._1, t._2.sum))

      println("groupByKey:", c0.toDebugString)

      c0.saveAsTextFile(Paths.get(args(1), "groupByKey").toString)

    }, "groupByKey")

  }

}