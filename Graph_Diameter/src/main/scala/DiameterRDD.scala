import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DiameterRDD extends Cmd {

  override def analysis_diameter(spark: SparkSession, path: String, k: Double, strict: Boolean = false): Unit = {

    val edges = load_random_sample(spark.sparkContext, path, k, strict)

    print(join_loop(edges))
  }


  private def load_random_sample(sc: SparkContext, path: String, k: Double, strict: Boolean = false): RDD[(Int, Int)] = {

    val edges = Utils.read_rdd(path, sc)
    var edges_v1: RDD[(Int, Int)] = null

    if (strict) {
      val nodes = edges.flatMap(x => Seq((x._1, 0), (x._2, 0))).reduceByKey((_, _) => 0)
      val nodes_v1 = nodes.sample(false, k)
      val nodes_v2 = sc.broadcast(nodes_v1.collect().toMap)
      edges_v1 = edges.filter(x => nodes_v2.value.contains(x._1) && nodes_v2.value.contains(x._2))
    } else {
      edges_v1 = edges.sample(false, k)
    }

    // edges_v1.checkpoint()

    edges_v1.cache()
  }


  private def join_loop(edges: RDD[(Int, Int)]): RDD[(Int, (Int, Int, Seq[Int]))] = {

    val all_edges = edges.map(x => (x._1, (x._2, 1, Seq[Int](x._1))))

    var join_result = edges.map(x => (x._2, (x._1, 1, Seq[Int](x._2))))

    var all_join_result = all_edges

    do {
      join_result = join_result.join(all_edges)
        .filter(x => {
          val x1 = x._2._1
          val x2 = x._2._2

          !x1._3.contains(x2._1)
        })
        .map(x => {
          val x1 = x._2._1
          val x2 = x._2._2

          // leaf node,  src node,  cost,  paths
          (x2._1,
            (x1._1,
              x2._2 + x1._2,
              Seq.concat(x1._3, Seq[Int](x._1), x2._3)
            )
          )
        })

      println("x000")
      print(join_result)


      all_join_result = all_join_result.union(join_result).distinct()

    } while (join_result != null && !join_result.isEmpty())

    all_join_result
  }


  def print(result: RDD[(Int, (Int, Int, Seq[Int]))]): Unit = {
    result.foreach(x => {
      val f = x._1
      val t = x._2._1
      val c = x._2._2
      // val p = x._2._3.take(1000).toSeq.mkString(",")

      println(f"$f%d\t$t%d\t$c%d")
    })
  }
}
