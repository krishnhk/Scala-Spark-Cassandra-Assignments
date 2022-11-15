package SparkCassandraConnector.WriteData

import com.datastax.spark.connector._
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random.nextInt

/**
 * * This program generates user data randomly with given functions
 * * The generated data is populated to Cassandra database using Spark Cassandra Connector
 */
object WriteDataToCassandraTable extends Serializable {
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

    logger.info("Spark Context begins")

    // * Generating list of data with UserHistory Case class
    implicit val data: List[UserHistory] = (1 to 20000).map(x => {
      UserHistory(getCountry, x, generateName, getAge, generatePhoneNumber, generatePurchaseList)
    }).toList
    logger.info("User data is generated")

    // * Writing the data to Cassandra table "purchaselog"
    def writeDataToCassandra(implicit data: List[UserHistory]): Unit = {
      val newData = sparkContext.parallelize(data)
      newData.saveToCassandra(keyspace, table)

      logger.info("Data is populated to the Cassandra table")
    }

    writeDataToCassandra

    logger.info("Spark Context ends")
    sparkContext.stop()
  }

  //  * ---------------------------------------------- Private Methods ---------------------------------------------- *//

  // * Getting a country name randomly from the country list
  private def getCountry: String = {
    List("France", "United States", "China", "Spain", "Italy",
      "Turkey", "United Kingdom", "Germany", "Russia", "Malaysia",
      "Mexico", "Austria", "Hong Kong", "Ukraine", "Thailand",
      "Saudi Arabia", "Greece", "Canada", "Poland", "Macao")(nextInt(20))
  }

  // * Generate a name randomly with only alphabets
  // * The name is limited to the length between 5 and 15
  private def generateName: String = {
    val (minNameLength, maxNameLength) = (5, 15)
    val nameLength = minNameLength + nextInt(maxNameLength - minNameLength)
    val name = (0 until nameLength).map(_ => ('a' to 'z') (nextInt(26))).mkString.capitalize
    name
  }

  // * Getting age between the range of 25 and 60
  private def getAge: Int = {
    val (minAge, maxAge) = (26, 60)
    minAge + nextInt(maxAge - minAge)
  }

  // * Generating phone number
  private def generatePhoneNumber: String = {
    val areaCode = s"${(0 until 3).map(_ => nextInt(9)).mkString}"
    val exchangeCode = s"${(0 until 3).map(_ => nextInt(9)).mkString}"
    val lineNumber = s"${(0 until 4).map(_ => nextInt(9)).mkString}"
    s"$areaCode-$exchangeCode-$lineNumber"
  }

  // * Generating purchase list with id, item_name, item_ price with Case Class Purchase
  // * The list of items is limited between 2 and 15
  private def generatePurchaseList: List[Purchase] = {
    val (minLimit, maxLimit) = (2, 15)
    val purchaseCount = minLimit + nextInt(maxLimit - minLimit)
    val purchaseList = (1 to purchaseCount).map(x => {
      Purchase(x, generateName, getPrice)
    })
    purchaseList.toList
  }

  // * Getting price of the item between 10 and 2000
  private def getPrice: Int = {
    val (minPrice, maxPrice) = (10, 2000)
    val price = minPrice + nextInt(maxPrice - minPrice)
    price
  }
}
