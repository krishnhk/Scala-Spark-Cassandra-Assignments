import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.util.UUID
import scala.util.Random.nextInt

object ExerciseTwo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise Two")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    logger.info("Spark Session Begins")

    def generateRobotType: String = List("AMR", "AGV", "Humanoid", "Hybrid", "Articulated Robot", "Cobot")(nextInt(6))

    def generateRobotUUID: String = UUID.randomUUID.toString

    def generateRobotID = s"CR-${(0 until 2).map(_ => nextInt(10)).mkString}"

    def checkMobility(model: String): String = {
      model match {
        case "AMR" | "AGV" | "Humanoid" | "Hybrid" => "Mobile"
        case "Articulated Robot" | "Cobot" => "Immobile"
      }
    }
    // * Generating details of 10 robots
    val robot_IDs = (0 until 10).map(_ => (generateRobotID, generateRobotType, generateRobotUUID))

    logger.info("\n\n==> Displaying an RDD of Case Class\n")
    val robotRDD = spark.sparkContext.parallelize(robot_IDs)
    val newRobotRDD = robotRDD.map(row => (row._1, row._2, checkMobility(row._2), row._3))
    val robotArmyRDD = newRobotRDD.map(row => RobotRecord(row._1, row._2, row._3, row._4))
    logger.info(robotArmyRDD.collect.mkString(" \n"))


    logger.info("\n\n==> Displaying an RDD of class\n")
    val robotRDD2 = spark.sparkContext.parallelize(robot_IDs)
    val newRobotRDD2 = robotRDD2.map(row => (row._1, row._2, row._3))

    // * Creating RDD of Robot class
    val robotArmyRDD2 = newRobotRDD2.map(row => new Robot(row._1, row._2, checkMobility(row._2), row._3))
    logger.info(robotArmyRDD2.collect.mkString(" \n"))

    logger.info("\n\n==> Displaying Data Frame\n")
    val robotDF = spark.createDataFrame(robot_IDs).toDF("Robot ID", "Type", "Robot UUID")
    robotDF.show(false)


    logger.info("\n\n==> Displaying Data Set")
    val robotDS = robot_IDs.map(row => RobotRecord(row._1, row._2, checkMobility(row._2), row._3)).toDS()
    robotDS.show(false)

    spark.stop()
  }

}
