package SparkCassandraConnector.WriteData

case class UserHistory(country: String, id: Int, username: String, age: Int, phonenumber: String, purchases: List[Purchase])
