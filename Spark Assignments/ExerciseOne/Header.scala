import java.time.LocalDate

case class Header(camp_ID: Int, individual_ID: Long, passCode: String, date_Of_Birth: LocalDate,cutOff:Int,major:Boolean)
