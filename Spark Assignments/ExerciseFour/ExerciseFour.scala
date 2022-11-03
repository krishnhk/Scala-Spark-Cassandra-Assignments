import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ExerciseFour extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise Two")
      .master("local[*]")
      .getOrCreate()

    logger.info("Spark Session Begins")
    import spark.implicits._

    val cities = List(
      ("Tamil Nadu", "India"),
      ("Tokyo", "Japan"),
      ("London", "England"),
      ("Amsterdam", "Netherlands"),
      ("Paris", "France"))

    val continents = List(
      ("Japan", "Asia", "Non-Polar"),
      ("England", "Europe", "Non-Polar"),
      ("France", "Europe", "Non-Polar"),
      ("India", "Asia", "Non-Polar"),
      ("Green Land", "Arctic", "Polar"))

    def joinRDD(): Unit = {

      val cityBaseRDD = spark.sparkContext.parallelize(cities)

      // * Created a key-tuple pair with primary key - country
      val cityRDD = cityBaseRDD.keyBy(x => x._2)

      val continentBaseRDD = spark.sparkContext.parallelize(continents)

      // * Created a key-tuple pair with primary key - country
      val continentRDD = continentBaseRDD.keyBy(x => x._1)

      // *  RDD inner join
      val innerJoinedRDD = cityRDD.join(continentRDD)
      innerJoinedRDD.foreach(println)

      // * RDD Left outer join
      val leftOuterRDD = cityRDD.leftOuterJoin(continentRDD)
      leftOuterRDD.foreach(println)
    }

    joinRDD()
    
    def joinDataFrame(): Unit = {

      val cityDF = cities.toDF("City", "Country")
      val continentDF = continents.toDF("Country", "Continent", "Region")
      val dfJoinExpr = cityDF.col("Country") === continentDF.col("Country")
      val dfJoinType = "inner"

      cityDF.join(continentDF, dfJoinExpr, dfJoinType)
        .drop(cityDF.col("Country"))
        .show()
    }

    joinDataFrame()

    def joinDataSet(): Unit = {
      
      val cityList = cities.map(row => City(row._1, row._2))
      val cityDS = spark.createDataset(cityList)

      val continentList = continents.map(row => Continent(row._1, row._2, row._3))
      val continentDS = spark.createDataset(continentList)

      val dsRightJoinType = "right"
      val dsInnerJoinType = "inner"
      val dsJoinExpr = cityDS("country") === continentDS("country")

      // * Dataset Right join
      cityDS.joinWith(continentDS, dsJoinExpr, dsRightJoinType)
        .map {
          // * Null produced from the cityDs in the join operation will be marked as Unknown City
          case (null, y) => CityContinent("Unknown City", y.continent, y.region)
          // * Null produced from the continentDs in the join operation will bw marked as Unknown Continent & Unknown region
          // * To avoid null pointer exception
          case (x, null) => CityContinent(x.city, "Unknown Continent", "Unknown Region")
          case (x, y) => CityContinent(x.city, y.continent, y.region)
        }
        .withColumnRenamed("city", "City")
        .withColumnRenamed("continent", "Continent")
        .withColumnRenamed("region", "Region")
        .show()
      
      // * Dataset inner join
      cityDS.joinWith(continentDS, dsJoinExpr, dsInnerJoinType)
        .map {
          case (null, y) => CityContinent("Unknown City", y.continent, y.region)
          case (x, null) => CityContinent(x.city, "Unknown Continent", "Unknown Region")
          case (x, y) => CityContinent(x.city, y.continent, y.region)
        }
        .withColumnRenamed("city", "City")
        .withColumnRenamed("continent", "Continent")
        .withColumnRenamed("region", "Region")
        .show()
    }

    joinDataSet()

    logger.info("Spark session ends")
    spark.stop()
  }
}
