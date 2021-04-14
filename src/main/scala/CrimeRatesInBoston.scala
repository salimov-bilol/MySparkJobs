import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CrimeRatesInBoston {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Crimes in Boston")
    conf.set("spark.sql.warehouse.dir", "D:\\IdeaProjects\\MySparkJobs\\spark-warehouse")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val df1 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:\\IdeaProjects\\MySparkJobs\\data\\crime.csv")

    val df2 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:\\IdeaProjects\\MySparkJobs\\data\\offense_codes.csv")

    import spark.implicits._

    val crimes = df1.select($"OFFENSE_CODE", $"DISTRICT", $"STREET")
    val offenseCodes = df2.select($"CODE".as("OFFENSE_CODE"), $"NAME").dropDuplicates("OFFENSE_CODE")

    crimes.show(false)
    offenseCodes.show(truncate = false)

  }

}
