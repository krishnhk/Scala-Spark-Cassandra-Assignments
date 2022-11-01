
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.temporal.ChronoUnit._
import scala.util.Random.nextInt


object ExerciseOne extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Exercise One")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    logger.info("Spark Session Begins")

    // * Generate RandomInt between the given range
    def generateCutOff: Int = {
      val start = 75
      val end = 150
      start + nextInt(end - start)
    }

    def generatePIN = s"${('A' to 'Z') (nextInt(26))}${(0 until 4).map(_ => nextInt(10)).mkString}"

    def generateIndividualID = (0 until 12).map(_ => nextInt(10)).mkString

    // * Random Date between the given range
    def generateDate(from: LocalDate = LocalDate.of(2001, 1, 1), to: LocalDate = LocalDate.of(2005, 12, 31)): LocalDate = {
      val diff = DAYS.between(from, to)
      val date = from.plusDays(nextInt(diff.toInt))
      date
    }

    def checkMajor(date: LocalDate): Boolean = {
      val today = LocalDate.now()
      val diff = YEARS.between(date, today)
      if (diff >= 18) true else false
    }

    val campID = (0 until 10).map(_ => nextInt(120))

    // * Adding 100 rows to each Camp ID
    val campLog = campID.flatMap(x => (0 to 99).map(_ => (x, generateIndividualID, generatePIN, generateDate(), generateCutOff)))

    val campLogRDD = spark.sparkContext.parallelize(campLog)

    // * Adding an additional column using data from existing column

    val verifiedCampLogRDD = campLogRDD.map(row => Header(row._1, row._2.toLong, row._3, row._4, row._5, checkMajor(row._4)))
    val campDS = spark.createDataset(verifiedCampLogRDD)


    // * Counting number of rows
    val countDs = campDS.count()
    logger.info(countDs)

    // * Finding max int value by grouping camp_ID
    campDS.groupBy("camp_ID")
      .agg(max("individual_ID").as("Max ID"))
      .show()

    // * Counting number of rows with int greater than 100
    val higherCount = campDS.where("cutOff>100").count()
    logger.info(higherCount)

    // * Multiplying the cutoff coulmn by 2
    campDS.withColumn("cutOff", col("cutOff") * 2).show()

    spark.stop()

  }

}
