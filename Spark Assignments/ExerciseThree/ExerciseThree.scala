import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.util.UUID
import scala.util.Random.nextInt

object ExerciseThree extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise Two")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    logger.info("Spark Session Begins")

    def runExercise(): Unit = {

      // * Creating record of 10 rows
      val robot_IDs = (0 until 10).map(_ => (generateRobotID, getRobotType, generateRobotUUID))

      // * Data to DataFrame
      val robotDF = robot_IDs.toDF("RobotID", "Type", "UUID")
      robotDF.show(false)

      // * DataFrame to DataSet
      val robotDS = robotDF.as[RobotRecord]
      robotDS.show(false)

      // * DataSet to RDD
      val robotRDD = robotDS.rdd.cache()
      logger.info(robotRDD.collect().mkString(" \n"))

      // * RDD to DataSet
      val rddToDs = spark.createDataset(robotRDD)
      rddToDs.show(false)

      // * RDD to DataFrame
      val rddToDf = robotRDD.toDF
      rddToDf.show(false)

      robotRDD.unpersist()
    }

    runExercise()

    logger.info("Spark session ends")
    spark.stop()
  }
  
  private def getRobotType: String = List("AMR", "AGV", "Humanoid", "Hybrid", "Articulated Robot", "Cobot")(nextInt(6))

  private def generateRobotUUID: String = UUID.randomUUID.toString

  private def generateRobotID = s"CRC-${(0 until 2).map(_ => nextInt(10)).mkString}"
}
