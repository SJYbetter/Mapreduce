import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DiameterDF extends Cmd {

  override def analysis_diameter(spark: SparkSession, s: String, i: Double, b: Boolean) {
    val edges = load_random_sample(spark, s, i, b)

    val all_paths = join_loop(edges)

    // all_paths.sort(col("v1"), col("v2")).show()

    val shortest_path = all_paths.groupBy(col("v1"), col("v2")).min("cost").cache()

    shortest_path.sort(col("v1"), col("v2")).show()

    // shortest_path.agg(col("min(cost)"))

    shortest_path.select(max("min(cost)")).show() // .groupBy("min(cost)").max("max_cost").show()
  }

  private def load_random_sample(spark: SparkSession, path: String, k: Double, strict: Boolean = false): DataFrame = {
    import spark.implicits._

    // load data
    val edges = Utils.read_rdd(path, spark.sparkContext).toDF("v1", "v2")

    var edges_v1: DataFrame = null

    if (strict) {
      // get all users
      val nodes = edges.rdd.flatMap(f => Seq(f.get(0).asInstanceOf[Int], f.get(1).asInstanceOf[Int])).distinct().cache()

      // run k source sample
      val sample_nodes = nodes.sample(false, k).toDF()

      //sample_nodes.show()

      // find all edges for samples
      edges_v1 = sample_nodes.join(edges, sample_nodes("value") === edges("v1") || sample_nodes("value") === edges("v2"))

      //edges.show()

    } else {
      // just return k source sample on edges
      edges_v1 = edges.sample(false, k)
    }

    //edges_v1.checkpoint()

    edges_v1.cache()
  }

  private def join_loop(edges: DataFrame): DataFrame = {

    var all_edges = edges.withColumn("cost", lit(1))
      .withColumn("p", lit(null))

    var join_result: DataFrame = all_edges.alias("f")

    var to = all_edges.alias("t")

    var all_join_result = all_edges

    do {
      join_result = join_result.alias("f").join(to, col("f.v2") === col("t.v1") && col("f.v1") =!= col("t.v2"))
        .select(
          col("f.v1"),
          col("t.v2").as("v2"),
          (col("f.cost") + col("t.cost")).as("cost"),
          concat_ws(",", col("f.p"), col("f.v2")).as("p")
        ).cache()

      all_join_result = all_join_result.union(join_result).distinct() 

    } while (join_result != null && !join_result.head(1).isEmpty)

    return all_join_result
  }

}
