import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test2 {
  def main(args: Array[String]) {
    val k: Int = 100

    val spark = SparkSession.builder()
      .appName("page_rank")
      .getOrCreate()

    val (pages, ranks) = generate_random_data(spark.sparkContext, k)

     page_rank_by_rdd(spark.sparkContext, pages, ranks, k)

    // page_rank_by_df(spark, pages, ranks, k)

    //page_rank_by_ds(spark, pages, ranks, k, 10)

    Console.out.print("press enter key to exit...")
    Console.in.read()

    spark.close()

    SparkSession.clearActiveSession()
  }


  // 1.1 to get the random data
  def generate_random_data(sc: SparkContext, k: Int): (RDD[(Int, Int)], RDD[(Int, Double)]) = {
    val kk = k * k


    var pages = sc.parallelize(Seq.range(0, k).flatMap(x =>
      Seq.range(x * k + 1, x * k + k)
        .flatMap(x1 => Seq((x1, x1 + 1)))
        .union(Seq(((x + 1) * k, 0)))
    )).cache()

    var ranks = sc.parallelize(Seq.range(1, kk + 1)
      .map(x => (x, 1.0 / kk))
      .union(Seq((0, 0.0)))).cache()

    if (pages.getNumPartitions < 2)
      pages = pages.repartition(2)

    if (ranks.getNumPartitions < 2)
      ranks = ranks.repartition(2)

    (pages, ranks)
  }

  // 1.2 by rdd
  def page_rank_by_rdd(sc: SparkContext, pages: RDD[(Int, Int)], ranks: RDD[(Int, Double)], k: Int, loop: Int = 10): Unit = {
    val kk = k * k
    var ranks_override = ranks

    // 1.2
    for (a <- 1 to loop) {
      // Join & Map
      val tmp1 = pages.join(ranks_override)

      val tmp2 = tmp1.map(x => x._2).reduceByKey(_ + _)

      val delta = tmp2.filter(x => x._1 == 0).map(x => (x._1, x._2 / kk)).collect()(0)

      // add delta value to tmp2
      val tmp3 = tmp2.map(x => (x._1, if (x._1 != 0) x._2 + delta._2 else x._2))

      println("tmp2")
      tmp2.foreach(println)

      ranks_override = ranks_override.leftOuterJoin(tmp3).map(x => (x._1, if (x._2._2.isDefined) x._2._2.get else x._2._1))
        .cache()

      // val delta = pages.filter(x => x._2 == 0).join(tmp2).map(x => (x._1, x._2._2))
      println("loop ", a, " total ", ranks_override.reduce((x, y) => (-1, x._2 + y._2))._2)
    }

    ranks_override.sortBy(v => v._2 * -1).take(100).foreach(println)
  }

  // 1.2 by dataframe
  def page_rank_by_df(spark: SparkSession, pages: RDD[(Int, Int)], ranks: RDD[(Int, Double)], k: Int, loop: Int = 10): Unit = {

    val kk = k * k
    val df_pages = spark.createDataFrame(pages).toDF("v1", "v2")
    var df_ranks = spark.createDataFrame(ranks).toDF("v1", "pr")

    for (a <- 1 to loop) {
      val tmp1 = df_pages.join(df_ranks, "v1")
      val tmp2 = tmp1.select("v2", "pr").groupBy("v2").sum("pr").withColumnRenamed("sum(pr)", "pr_sum")

      // calc delta
      val delta = tmp2.filter(tmp2.col("v2").equalTo(0)).take(1).head.getDouble(1) / kk

      // add delta
      val tmp3 = tmp2.withColumn("pr_1", when(tmp2("v2") === lit(0), tmp2.col("pr_sum")).otherwise(tmp2("pr_sum") + delta))

      val tmp4 = df_ranks.join(tmp3, df_ranks("v1") === tmp3("v2"), "left")
      df_ranks = tmp4.select(df_ranks("v1"), when(tmp3("v2").isNull, df_ranks("pr")).otherwise(tmp3("pr_1")).as("pr")).cache()

      df_ranks.show()


      val total_pr = df_ranks.select("pr").rdd.map(_ (0).asInstanceOf[Double]).reduce(_ + _)

      println("loop ", a, " total ", total_pr)

    }
  }

  case class page_t(v1: Int, v2: Int)

  case class rank_t(v2: Int, pr: Double)


  def page_rank_by_ds(spark: SparkSession, pages: RDD[(Int, Int)], ranks: RDD[(Int, Double)], k: Int, loop: Int = 10): Unit = {

    import spark.implicits._

    val kk = k * k

    val ds_pages = pages.map(x => page_t(v1 = x._1, v2 = x._2)).toDS()
    var ds_ranks = ranks.map(x => rank_t(v2 = x._1, pr = x._2)).toDS().cache()

    for (a <- 1 to loop) {
      val t0 = ds_pages.joinWith(ds_ranks, ds_pages("v1") === ds_ranks("v2"), "left")
      val t1 = t0.select(t0("_1.v2"), t0("_2.pr"))
      val t2 = t1.groupBy(t1("v2")).sum("pr").withColumnRenamed("sum(pr)", "pr_sum")

      val delta = t2.where(t2("v2") === 0).head().get(1).asInstanceOf[Double] / kk

      val t3 = t2.withColumn("pr_1", when(t2("v2") === lit(0), t2("pr_sum")).otherwise(t2("pr_sum") + delta))

      val t4 = ds_ranks.joinWith(t3, ds_ranks("v2") === t3("v2"), "left")

      t1.show()
      t2.show()
      t3.show()
      t4.show(truncate = false)

      ds_ranks = t4.select(t4("_1.v2"), when(t4("_2").isNull, t4("_1.pr")).otherwise(t4("_2.pr_1").as("pr")))
        .map(r => rank_t(v2 = r.get(0).asInstanceOf[Int], pr = r.get(1).asInstanceOf[Double]))
        .cache()

      ds_ranks.show()

      val total_pr = ds_ranks.select("pr").rdd.map(_ (0).asInstanceOf[Double]).reduce(_ + _)

      println("loop ", a, " total ", total_pr)
    }

  }

}
