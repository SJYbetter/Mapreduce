package cs6240.hw2

object ReplicatedJoinDS {

  def main(args: Array[String]) {

    val max_accepted_id = Integer.parseInt(args(2))

    Utils.run_sql("replicated join by rdd", sc => {

      import sc.implicits._

      val all_edges = Utils.read_rdd(args(0), sc.sparkContext)
        .filter(x => x._1 < max_accepted_id && x._2 <= max_accepted_id);

      val edges = all_edges.toDS()



      edges.show(10)
      //edges.join(edges, )
      // edges.map()

//      all_edges.collectAsMap().foreach(y => {
//        println(y)
//      })


      //ds1.collect
      //ds1.show()

      //      println(ds1.count())

      //      ds1.foreach(println)

    })

  }

}
