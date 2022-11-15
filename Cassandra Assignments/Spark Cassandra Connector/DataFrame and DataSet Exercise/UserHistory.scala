package SparkCassandraConnector.DFDSExercise

case class UserHistory(country: String, id: Int, username: Option[String], age: Option[Int], phonenumber: Option[String], purchases: List[Purchase])
