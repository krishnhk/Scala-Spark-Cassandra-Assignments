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
    logger.info("Spark Session Begins")


    def getRobotType: String = List("AMR", "AGV", "Humanoid", "Hybrid", "Articulated Robot", "Cobot")(nextInt(6))

    def generateRobotUUID = UUID.randomUUID.toString

    def generateRobotID = s"RB-${(0 until 2).map(_ => nextInt(10)).mkString}"

    import spark.implicits._

    // * Creating record of 10 rows
    val robot_IDs = (0 until 10).map(_ => (generateRobotID, getRobotType, generateRobotUUID))

    logger.info("\n\n==> Data to DataFrame\n")
    val robotDF = robot_IDs.toDF("RobotID", "Type", "UUID")
    robotDF.show(false)



    logger.info("\n\n==> DataFrame to DataSet\n")
    val robotDS = robotDF.as[RobotRecord]
    robotDS.show(false)



    logger.info("\n\n==> DataSet to RDD\n")
    val robotRDD = robotDS.rdd
    logger.info(robotRDD.collect().mkString(" \n"))



    logger.info("\n\n==> RDD to DataSet\n")
    val rddToDs  = spark.createDataset(robotRDD)
    rddToDs.show(false)



    logger.info("\n\n==> RDD to DataFrame\n")
    val rddToDf = robotRDD.toDF
    rddToDf.show(false)

  }


}
