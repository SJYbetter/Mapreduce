package cs6240.hw2

import scala.collection.mutable

object ReduceJoinRDD {

  def main(args: Array[String]) {

    val max_accepted_id = Integer.parseInt(args(2))

    Utils.run("ReduceJoinRDD", sc => {

      val edges = Utils.read_rdd(args(0), sc).filter(x => x._1 <= max_accepted_id && x._2 <= max_accepted_id)

      edges.collect


      val initialSet = mutable.HashSet.empty[Int]
      val addToSet = (s: mutable.HashSet[Int], v: Int) => s += v
      val mergePartitionSets = (p1: mutable.HashSet[Int], p2: mutable.HashSet[Int]) => p1 ++= p2
      val uniqueByKey = edges.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

      val outgoings = sc.broadcast(uniqueByKey.collectAsMap())

      val incomings = sc.broadcast(edges.map(x => (x._2, x._1))
        .aggregateByKey(mutable.HashSet.empty[Int])(
          (set, v) => set += v,
          (set1, set2) => set1 ++= set2
        ).collectAsMap())
    })


  }
}