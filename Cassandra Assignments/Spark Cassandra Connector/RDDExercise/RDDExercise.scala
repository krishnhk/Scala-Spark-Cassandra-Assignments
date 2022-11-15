package SparkCassandraConnector.RDDExercise

import com.datastax.spark.connector._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
/**
*  * This program reads data from Cassandra Database as RDD
*  * Manipulate the read data with the given operation
*  * To get total purchase amount of each user
*  * To get highest and lowest purchaser for each country
*/

object RDDExercise extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val cassandraHost = "127.0.0.1:9042"
    val keyspace = "mykeyspace"
    val table = "purchaselog"

    //  Configuring Spark Context
    val conf = new SparkConf(true)
      .set("org.apache.spark.cassandra", cassandraHost)
      .setAppName("RDD Exercise")
      .setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    logger.info("Spark Context Begins")

    // * Reading data from Cassandra table in the form of RDD[UserHistory]

    val readRDD = sparkContext.cassandraTable[UserHistory](keyspace, table)
    readRDD.foreach(println)
    logger.info("Data has been read from Cassandra table as RDD[UserHistory]")

    // * Reading data from the Cassandra Table with joinWithCassandraTable
    val joinWithCassandraRdd = sparkContext.cassandraTable(keyspace, table).joinWithCassandraTable(keyspace, "customerlog").on(SomeColumns("country", "id"))
    joinWithCassandraRdd.foreach(println)
    logger.info("Data has been read from Cassandra Table using joinWithCassandraTable")

    // * Reading  data from the cassandra table with where clause
    // * Only selecting users from Greece and  username and purchase
    val whereClauseRDD = sparkContext.cassandraTable[(Option[String], List[Purchase])](keyspace, table)
      .where("country = 'Greece' ")
      .select("username", "purchases")
    whereClauseRDD.foreach(println)

    // * Doing the above function with case class PurchaseLog
    val whereClauseRDDOfCaseClass = sparkContext.cassandraTable[PurchaseLog](keyspace, table)
    whereClauseRDDOfCaseClass.foreach(println)


    // * Manipulating operations are done in this method
    def manipulateRDD(): Unit = {

      //  *  Using where clause in the RDD read from Cassandra table to get specified 5 countries
      val manipulatingRDD = readRDD.where("country IN ('France','China','Italy','Thailand','Macao')")
      logger.info("RDD for 5 countries has been created")

      // * Calculating Total purchase amount of each user
      val purchaseExpense = manipulatingRDD.map(x => {
        val totalAmount = x.purchases.map(x => x.item_price).sum
        PurchaseCost(x.country, x.id, x.username, x.age, x.phonenumber, totalAmount)
      })
      logger.info("Purchase expense each user is calculated")

      // * Creating key - value with totalAmount as Primary Key
      val keyRddOfPurchaseExpense = purchaseExpense.keyBy(x => x.totalAmount)

      // * Calculating the max purchase amount for each country
      val maxExpense = purchaseExpense.groupBy(x => x.country)
        .flatMapValues(x => x.map(i => i.totalAmount))
        .reduceByKey(_ max _)

      // * Creating key - value pair in max Expense RDD with totalAmount as primary Key
      val keyRddOfMaxExpense = maxExpense.keyBy(x => x._2)

      // * Joining the keyRddOfMaxExpense and keyRddOfPurchaseExpense RDDs and getting the necessary user details
      val maxPurchaser = keyRddOfPurchaseExpense.rightOuterJoin(keyRddOfMaxExpense)
        .map(x => PurchaseCost(x._2._2._1, x._2._1.get.id, x._2._1.get.username, x._2._1.get.age, x._2._1.get.phonenumber, x._1))
      maxPurchaser.foreach(println)
      logger.info("Maximum purchaser list is created")

      // *  Calculating the min purchase amount for each country which is not zero
      val minExpense = purchaseExpense.filter(x => x.totalAmount > 0)
        .groupBy(x => x.country)
        .flatMapValues(x => x.map(i => i.totalAmount))
        .reduceByKey(_ min _)

      // * Creating key - value pair in min Expense RDD with totalAmount as primary Key
      val keyRddOfMinExpense = minExpense.keyBy(x => x._2)

      // * Joining the keyRddOfMinExpense and keyRddOfPurchaseExpense RDDs and getting the necessary user details
      val minPurchaser = keyRddOfPurchaseExpense.rightOuterJoin(keyRddOfMinExpense)
        .map(x => PurchaseCost(x._2._2._1, x._2._1.get.id, x._2._1.get.username, x._2._1.get.age, x._2._1.get.phonenumber, x._1))
      minPurchaser.foreach(println)
      logger.info("Minimum purchaser list is created")
    }
    // Calling the manipulateRDD method
    manipulateRDD()

    logger.info("Spark context ends")
    sparkContext.stop()
  }
}