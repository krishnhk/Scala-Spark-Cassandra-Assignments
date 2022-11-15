package SparkCassandraConnector.ReadData

case class UserHistory(country: String, id: Int, var username: String, var age: Option[Int], var phonenumber: String, var purchases: List[Purchase])
