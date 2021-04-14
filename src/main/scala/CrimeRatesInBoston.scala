import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CrimeRatesInBoston {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Crimes in Boston")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df1 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:/IdeaProjects/MySparkJobs/data/crime.csv")
    df1.printSchema()
    val df2 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:/IdeaProjects/MySparkJobs/data/offense_codes.csv")
    df2.printSchema()
    import spark.implicits._

    val crimes = df1
      .select($"OFFENSE_CODE", $"DISTRICT", $"STREET")
      .na
      .drop()

    val offenseCodes = df2
      .select($"CODE".as("OFFENSE_CODE"), $"NAME")
      .dropDuplicates("OFFENSE_CODE")

    val crimesCountByDistrictArray = crimes
      .groupBy($"DISTRICT")
      .count()
      .collect()

    var crimesCountByDistrictMap = Map[String, Long]()

    for (row <- crimesCountByDistrictArray) {
      crimesCountByDistrictMap += (row.getString(0) -> row.getLong(1))
    }

    val broadcastVariable = spark.sparkContext.broadcast(crimesCountByDistrictMap)

    import org.apache.spark.sql.functions._

    val myUDF = udf
    {
      district: String =>
      {
        val total = broadcastVariable.value.get(district)
        if (total.isEmpty) 0 else total.get
      }
    }

    val afterAggregation = crimes
      .groupBy($"DISTRICT", $"STREET", $"OFFENSE_CODE")
      .count()
      .withColumn("ratio", $"count" / myUDF($"DISTRICT") * 100)

    val afterJoining = offenseCodes
      .join(afterAggregation, "OFFENSE_CODE")

    val afterSelecting = afterJoining
      .select($"DISTRICT", $"STREET", $"NAME", $"count".as("COUNT"), $"ratio")
      .sort($"DISTRICT")
    afterSelecting.createTempView("afterSelecting")
    afterSelecting.groupBy($"DISTRICT").agg(sum($"ratio") as "ratioSum").createTempView("ratioCheck")
    spark.sql("select * from afterSelecting").show()
    spark.sql("select * from ratioCheck").show()
  }

}
