package SparkCassandraConnector.DFDSExercise

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *  * This program reads data from Cassandra Database as DataFrame
 *  * Manipulate the read data with the given operation
 *  * To get total purchase amount of each user
 *  * To get highest and lowest purchaser for each country
 */

object DFDSExercise extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val cassandraHost = "127.0.0.1:9042"
    val keyspace = "mykeyspace"
    val table = "purchaselog"

    // Configuring Spark Context
    val conf = new SparkConf(true)
      .set("org.apache.spark.cassandra", cassandraHost)
      .setAppName("RDD Exercise")
      .setMaster("local[*]")

    // Creating Spark Session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    logger.info("Spark Session Begins")

    import spark.implicits._

    // * Reading data from Cassandra Table as DataFrame
    val readDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()

    logger.info("DataFrame has been read from the CassandraTable")

    // * Converting Dataframe to Dataset using Case Class
    val readDS = readDF.as[UserHistory]

    // *  Reading username and purchases of certain  country using where clause
    // * Read data in the form of Dataset[(String,List[Purchase])]
    val whereClauseDS = readDF
      .where("country = 'France'")
      .select("username", "purchases").as[(String, List[Purchase])]

    // * Manipulating Operations are done in this method
    def manipulateDS(): Unit = {

      // * Reading data from cassandra table using where clause to pic users from 5 countries
      val manipulatingDS = readDF
        .where("country IN ('France','China','Italy','Thailand','Macao')").as[UserHistory]
      manipulatingDS.show()
      logger.info("Dataset of 5 countries is created")

      // * Calculating Total purchase amount of each user
      val purchaseExpense = manipulatingDS.map(x => {
        val totalAmount = x.purchases.map(x => x.item_price).sum
        PurchaseCost(x.country, x.id, x.username, x.age, x.phonenumber, totalAmount)
      })
      purchaseExpense.show(false)
      logger.info("Purchase expense each user is calculated")

      // * Calculating the max purchase amount for each country
      val maxExpense = purchaseExpense.groupByKey(x => x.country)
        .mapValues(x => x.totalAmount).reduceGroups(_ max _)

      // * Joining the maxExpense and purchaseExpense Datasets and getting the necessary user details
      val maxPurchaser = purchaseExpense.joinWith(maxExpense, purchaseExpense("country") === maxExpense("key") && purchaseExpense("totalAmount") === maxExpense("ReduceAggregator(int)"), "right").map({
        case (x, y) => PurchaseCost(x.country, x.id, x.username, x.age, x.phonenumber, y._2)
      })
      logger.info("Maximum purchaser list is created")
      maxExpense.show()
      maxPurchaser.show()

      // * Calculating the min purchase amount for each country
      val minExpense = purchaseExpense.where("totalAmount > 0")
        .groupByKey(x => x.country).mapValues(x => x.totalAmount).reduceGroups(_ min _)

      // * Joining the minExpense and purchaseExpense Datasets and getting the necessary user details
      val minPurchaser = purchaseExpense.joinWith(minExpense, purchaseExpense("country") === minExpense("key") && purchaseExpense("totalAmount") === minExpense("ReduceAggregator(int)"), "right").map({
        case (x, y) => PurchaseCost(x.country, x.id, x.username, x.age, x.phonenumber, y._2)
      })
      logger.info("Minimum purchaser list is created")
      minExpense.show()
      minPurchaser.show()
    }
    // Calling manipulateDS method
    manipulateDS()
    logger.info("Spark session ends")
    spark.stop()
  }
}
