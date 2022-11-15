package SparkCassandraConnector.ReadData

import com.datastax.spark.connector._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * * This program reads data from the Cassandra Database as RDD
 * * To make random phone number as null
 * * To make random age as null
 * * To make random username as null
 * * To make random purchase list as empty
 *
 */
object ReadDataFromCassandraTable extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val cassandraHost = "127.0.0.1:9042"
    val keyspace = "mykeyspace"
    val table = "purchaselog"

    //  Configuring Spark Context
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .setAppName("Cassandra Assignment")
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)


    //  Creating Spark Session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    logger.info("Spark session and Spark Context begins")

    import spark.implicits._

    // * Reading Data from Cassandra table
    val readingDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()
    logger.info("Data has been read from the Cassandra table")

    // * Converting DataFrame into RDD of User History
    implicit val readingRDD: RDD[UserHistory] = readingDF.as[UserHistory].rdd.cache()

    // * Setting 100 random Phone Number as Null
    def setPhoneNumberNull(implicit processingRDD: RDD[UserHistory]): Unit = {
      val processedRDD = processingRDD.takeSample(withReplacement = false, num = 100).map(x => {
        x.phonenumber = null
        x
      })
      logger.info("Random phone number is set as null")
      saveToCassandraTable(processedRDD)
    }

    setPhoneNumberNull

    // * Setting 100 random Age as Null
    def setAgeNull(implicit processingRDD: RDD[UserHistory]): Unit = {
      val processedRDD = processingRDD.takeSample(withReplacement = false, num = 100).map(x => {
        x.age = None
        x
      })
      logger.info("Random age is set as null")
      saveToCassandraTable(processedRDD)
    }

    setAgeNull

    // * Setting 500 random username as Null
    def setUserNameNull(implicit processingRDD: RDD[UserHistory]): Unit = {
      val processedRDD = processingRDD.takeSample(withReplacement = false, num = 500).map(x => {
        x.username = null
        x
      })
      logger.info("Random username is set as null")
      saveToCassandraTable(processedRDD)
    }

    setUserNameNull

    // * Making 100 random purchase list as empty
    def makePurchaseListEmpty(implicit processingRDD: RDD[UserHistory]): Unit = {
      val processedRDD = processingRDD.takeSample(withReplacement = false, num = 100).map(x => {
        x.purchases = List.empty
        x
      })
      logger.info("Random purchase list is made as empty")
      saveToCassandraTable(processedRDD)
    }

    makePurchaseListEmpty

    // * Writing the updated data to Cassandra Table
    def saveToCassandraTable(processedRDD: Array[UserHistory]): Unit = {
      val rdd = sparkContext.parallelize(processedRDD)
      rdd.saveToCassandra(keyspace, table)
      logger.info("Updated data is populated to the Cassandra Table")
    }

    readingRDD.unpersist()

    logger.info("Spark Session and Spark Context ends")
    spark.stop()
    sparkContext.stop()
  }
}
