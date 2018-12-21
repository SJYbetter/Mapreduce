package cs6240.hw2

import java.nio.file.Paths

object FoldByKey {

  def main(args: Array[String]) {
    Utils.run(sc => {
      val edges = sc.textFile(args(0)).map(line => (line.split(",")(0), 1))

      // paritally applied function && Currying
      // https://blog.csdn.net/bluishglc/article/details/51042940
      val c1 = edges.foldByKey(0)((x, y) => x + y)

      println("foldByKey:", c1.toDebugString)

      c1.saveAsTextFile(Paths.get(args(1), "foldByKey").toString)

    }, "foldByKey")
  }
}