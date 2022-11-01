import scala.annotation.tailrec

object SuperDigit extends App {
 val input = scala.io.StdIn.readLine().split(" ")
 var n = input(0)
 var k = input(1).toInt
 def getLastInt(str: String) = ("" + str.charAt(str.length - 1)).toInt
 @tailrec
 def reduceNumber(in: String): Int = {
   @tailrec
   def addDigits(x: String, ac: Int = 0): Int = {
     if (x.isEmpty) ac
     else addDigits(x.substring(0, x.length - 1), ac + getLastInt(x))
   }
   val sum = addDigits(in)
   if (sum < 10) sum
   else reduceNumber(sum.toString)
 }
 val res = reduceNumber((reduceNumber(n) * k).toString)
 println(res)
}
