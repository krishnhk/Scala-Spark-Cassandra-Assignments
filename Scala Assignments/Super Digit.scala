import scala.annotation.tailrec

class NegativeNumberException(exceptionNote : String) extends Exception(exceptionNote)

object String_SuperDigit extends App {
  val input = scala.io.StdIn.readLine().split(" ")
  var n = input(0)
  var k = input(1).toInt

  // * Negative input throw NegativeNumberException
  @throws(classOf[NegativeNumberException])
  def checkNegative(num: String): Unit = {
    if (BigInt(num) < 0) {
      throw new NegativeNumberException("Negative Input")
    }
  }
  checkNegative(n)
  checkNegative(k.toString)


  // * Pick the last digit from the number
  def getLastInt(str: String) = str.charAt(str.length - 1).toString.toInt

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
