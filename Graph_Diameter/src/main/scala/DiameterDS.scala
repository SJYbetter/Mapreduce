
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object DiameterDS extends Cmd {

  case class follow_t(v1: Int, v2: Int)

  case class path_t(v1: Int, v2: Int, cost: Int, p: String)

  override def analysis_diameter(spark: SparkSession, path: String, k: Double, strict: Boolean = false): Unit = {
    val edges = load_random_sample(spark, path, k, strict)


    val all_paths = join_loop(edges, spark)

    println("result: ")

    all_paths.sort(col("v1"), col("v2")).show(1000)
  }


  private def load_random_sample(spark: SparkSession, path: String, k: Double, strict: Boolean = false): Dataset[follow_t] = {
    import spark.implicits._

    // load data
    val edges = Utils.read_rdd(path, spark.sparkContext).map(x => follow_t(x._1, x._2)).toDS()

    var edges_v1: Dataset[follow_t] = null

    if (strict) {
      // get all users
      val nodes = edges.flatMap(f => Seq(f.v1, f.v2)).distinct().cache()

      // run k source sample
      val sample_nodes = nodes.sample(k)

      // find all edges for samples
      edges_v1 = sample_nodes.joinWith(edges, sample_nodes("value") === edges("v1") || sample_nodes("value") === edges("v2")).select(col("_2").as[follow_t])

    } else {
      // just return k source sample on edges
      edges_v1 = edges.sample(false, k)
    }

    //edges_v1.checkpoint()

    edges_v1.cache()
  }

  private def join_loop(edges: Dataset[follow_t], spark: SparkSession): Dataset[path_t] = {
    import spark.implicits._

    val all_edges = edges.map(f => path_t(f.v1, f.v2, 1, "")).cache()

    var join_result: Dataset[path_t] = all_edges.alias("f")

    var all_join_result = all_edges

    //    next_step.show()
    //    join_result.show()
    do {
      val join0 = join_result.alias("f").joinWith(all_edges.alias("t"),
        col("f.v2") === col("t.v1") && col("f.v1") =!= col("t.v2"))

      join_result = join0.map(x => path_t(x._1.v1, x._2.v2, x._1.cost + x._2.cost,
        Seq(x._1.p, x._1.v2.toString, x._2.p).filter(x => x != "").mkString(",")))


      join_result.show()

      all_join_result = all_join_result.union(join_result).distinct() // .sort(col("v1"), col("v2")).show(10000)

    } while (join_result != null && !join_result.head(1).isEmpty)

    all_join_result
  }
}
