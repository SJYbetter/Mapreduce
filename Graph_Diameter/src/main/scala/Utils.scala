import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Utils {


  def run(name: String, callback: (SparkContext) => Unit, waitTimeout: Int = 120000) {
    val conf = new SparkConf().setAppName(name)
    val sc = new SparkContext(conf)

    callback(sc)

    //    if (waitTimeout > 0)
    //      Thread.sleep(waitTimeout)
    read_quit_key()

    sc.stop()
  }

  def run_sql(name: String, callback: (SparkSession) => Unit): Unit = {
    val spark = SparkSession.builder().appName(name)
      //.config("spark.sql.crossJoin.enabled", true)
      .getOrCreate()

    callback(spark)

    read_quit_key()
    spark.close()
  }

  def read_rdd(path: String, sc: SparkContext): RDD[(Int, Int)] = {
    val edges = sc.textFile(path).map(line => {
      val y = line.split(",")
      (Integer.parseInt(y(0)), Integer.parseInt(y(1)))
    })

    edges
  }

  private def read_quit_key(): Unit = {

    while (System.in.read() != '\n') {
      System.out.println("press enter to exit....")
    }
  }
}
