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

    def runExercise(): Unit = {

      // * Generating details of 10 robots
      val robot_IDs = (0 until 10).map(_ => (generateRobotID, generateRobotType, generateRobotUUID))

      // * Converting IndexedSeq to RDD
      val robotRDD = spark.sparkContext.parallelize(robot_IDs).cache()

      // * Creating RDD of case class

      val robotArmyRDD = robotRDD.map(row => RobotRecord(row._1, row._2, checkMobility(row._2), row._3))
      logger.info(robotArmyRDD.collect.mkString(" \n"))

      // * Creating RDD of class
      val normalRobotRDD = robotRDD.map(row => new Robot(row._1, row._2, checkMobility(row._2), row._3))
      logger.info(normalRobotRDD.collect.mkString(" \n"))

      // * Creating a DataFrame
      val robotDF = robot_IDs.toDF("Robot ID", "Type", "Robot UUID")
      robotDF.show(false)

      // * Creating a DataSet
      val robotDS = robot_IDs.map(row => RobotRecord(row._1, row._2, checkMobility(row._2), row._3)).toDS()
      robotDS.show(false)

      robotRDD.unpersist()
    }
    runExercise()

    logger.info("Spark Session ends")
    spark.stop()
  }
  
  private def generateRobotType: String = List("AMR", "AGV", "Humanoid", "Hybrid", "Articulated Robot", "Cobot")(nextInt(6))

  private def generateRobotUUID: String = UUID.randomUUID.toString

  private def generateRobotID = s"CR-${(0 until 2).map(_ => nextInt(10)).mkString}"

  private def checkMobility(model: String): String = {
    model match {
      case "AMR" | "AGV" | "Humanoid" | "Hybrid" => "Mobile"
      case "Articulated Robot" | "Cobot" => "Immobile"
    }
  }
}
