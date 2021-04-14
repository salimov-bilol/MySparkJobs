import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AggregationsDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Aggregations Demo")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val numbers = spark.range(100)

    val afterGrouping = numbers.
      groupBy('id % 2 as "group").
      agg(
        sum($"id") as "sum",
        count($"id") as "count",
        collect_list($"id") as "numbers"
      )

    val afterJoining = numbers.join(afterGrouping).where($"group" === $"id" % 2)

    afterJoining.show()

  }
}
