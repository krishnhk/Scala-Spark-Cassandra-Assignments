import org.apache.log4j.Logger
import org.apache.spark.sql._
import java.time.LocalDate
import java.time.temporal.ChronoUnit._
import scala.util.Random.nextInt

object ExerciseOne extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // * Generate RandomInt between the given range
  private def generateCutOff: Int = {
    val start = 75
    val end = 150
    start + nextInt(end - start)
  }

  // * Check whether the candidate is major or minor
  private def checkMajor(date: LocalDate): String = {
    val today = LocalDate.now()
    val diff = YEARS.between(date, today)
    if (diff >= 18) "Major" else "Minor"
  }

  // * Generate PIN of one character and four digit number
  private def generatePIN = s"${('A' to 'Z') (nextInt(26))}${(0 until 4).map(_ => nextInt(10)).mkString}"

  // * Random Date between the given range
  private def generateDate(fromDate: LocalDate = LocalDate.of(2001, 1, 1), toDate: LocalDate = LocalDate.of(2005, 12, 31)): LocalDate = {
    val diff = DAYS.between(fromDate, toDate)
    val date = fromDate.plusDays(nextInt(diff.toInt))
    date
  }

  // * Generate number of 12 digit
  private def generateIndividualID: String = (0 until 12).map({
    val start = 1
    val end = 10
    _ => start + nextInt(end - start)
  }).mkString

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise One")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    logger.info("Spark Session Begins")

    def runExercise(): Unit = {

      val campLog = (0 until 10).flatMap(_ => {
        val campNo = nextInt(120)
        val record = (0 until 100).map(_ => (campNo, generateIndividualID, generatePIN, generateDate(), generateCutOff))
        record
      })

      val campLogRDD = spark.sparkContext.parallelize(campLog)

      // * Adding an additional column using data from existing column
      val verifiedCampLogRDD = campLogRDD.map(row => Header(row._1, row._2.toLong, row._3, row._4, row._5, checkMajor(row._4)))
      val campDS = spark.createDataset(verifiedCampLogRDD).cache()

      // * Counting number of rows
      val countDs = campDS.count()
      logger.info(countDs)

      // * Finding max int value by grouping camp_ID
      campDS.groupByKey(x => x.camp_ID)
        .mapValues(x => x.individual_ID)
        .reduceGroups(_ max _)
        .show()

      // * Counting number of rows with int greater than 100
      val higherCount = campDS
        .filter(x => x.cutOff > 100)
        .count()
      logger.info(higherCount)

      // * Multiplying the cutoff column by 2
      campDS.map(x => Header(x.camp_ID, x.individual_ID, x.passCode, x.date_Of_Birth, x.cutOff * 2, x.major)).show()
      
      campDS.unpersist()
    }
    runExercise()
    
    logger.info("Spark Session ends")
    spark.stop()
  }
}
