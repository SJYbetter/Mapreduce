import org.apache.spark.sql.SparkSession

trait Cmd {


  def main(args: Array[String]): Unit = {

    val input: String = args(0)
    val k: Double = java.lang.Double.parseDouble(args(1))
    val strict = java.lang.Boolean.parseBoolean(args(2))


    Utils.run_sql("graph_diameter", (spark) => {

      analysis_diameter(spark, input, k, strict)
    })


  }

  def analysis_diameter(spark: SparkSession, s: String, i: Double, b: Boolean)

}


