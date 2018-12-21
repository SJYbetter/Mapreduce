package cs6240.hw2

import java.util

import scala.collection.mutable

object ReplicatedJoinRDD {

  def main(args: Array[String]) {

    val max_accepted_id = Integer.parseInt(args(2))

    Utils.run("replicated join by rdd", sc => {

      val edges = Utils.read_rdd(args(0), sc)
        .filter(x => x._1 < max_accepted_id && x._2 <= max_accepted_id);


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


      val result = edges.flatMap(x => {
        val r0 = outgoings.value.get(x._2)
        val r1 = incomings.value.get(x._1)


        val set1 = if (r0.isEmpty) mutable.HashSet.empty[Int] else r0.get
        val set2 = if (r1.isEmpty) mutable.HashSet.empty[Int] else r1.get


        val result = new util.ArrayList[Int]()
        set1.foreach(y => {
          if (set2.contains(y))
            result.add(y)
        })

        val lst = result.toArray().map(z => {
          val zz = Array(x._1.asInstanceOf[Int], x._2.asInstanceOf[Int], z.asInstanceOf[Int])

          ((zz.sorted.mkString(","), 1))
        })

        (lst)
      }).reduceByKey((_, _) => 1)


      val counters =  result.map(_ => (0, 1)).reduceByKey(_ + _).collect()


      println("incoming ******************")
      incomings.value.foreach(println)


      println("outgoings ******************")
      outgoings.value.foreach(println)

      result.foreach(println)

      result.saveAsTextFile(args(1))

      println("path2 count ******************")
      counters.foreach(println)
    })
  }
}
