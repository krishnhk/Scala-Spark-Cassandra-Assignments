import scala.annotation.tailrec

class InvalidInputException(exceptionNote:String) extends Exception(exceptionNote)

class InputFormatException (exceptionNote:String) extends Exception(exceptionNote)

object SubStringSearching extends App {
  val text = scala.io.StdIn.readLine()
  val pattern = scala.io.StdIn.readLine()

  @throws(classOf[InvalidInputException])
  def checkInputs(str: String, subst: String): Unit = {
    if (subst.length > str.length) throw new InvalidInputException("Invalid Input")
  }

  checkInputs(text, pattern)

  def checkInputFormat(str: String): Unit = {
    val letters = str.map(x => x.isLetter).reduce(_ && _)
    val lowerCase = str.map(x => x.isLower).reduce(_ && _)
    if (!(lowerCase && letters)) throw new InputFormatException("Given input is not lowercase")
  }

  checkInputFormat(text)
  checkInputFormat(pattern)

  val lps = Array.fill(pattern.length)(0)


  // * Create Longest Prefix Suffix Array
  @tailrec
  def calculateLps(str: String, i: Int, j: Int, lps: Array[Int]): Unit = {
    if (j == str.length) lps
    else if (str.charAt(i) != str.charAt(j) && i == 0) {
      lps(j) = 0
      calculateLps(str, i, j + 1, lps)
    }
    else if (str.charAt(i) == str.charAt(j)) {
      lps(j) = i + 1
      calculateLps(str, i + 1, j + 1, lps)
    }
    else if (str.charAt(i) != str.charAt(j) && i != 0) calculateLps(str, i = lps(i - 1), j, lps)
  }

  calculateLps(pattern, 0, 1, lps)

  // * Matching the pattern with Text
  @tailrec
  def searchSubstring(str: String, substr: String, i: Int, j: Int, lps: Array[Int]): Unit = {
    if (i == str.length) println("NO")
    else if (str.charAt(i) == substr.charAt(j) && j == substr.length - 1) println("YES")
    else if (str.charAt(i) == substr.charAt(j)) searchSubstring(str, substr, i + 1, j + 1, lps)
    else if (str.charAt(i) != substr.charAt(j) && j == 0) searchSubstring(str, substr, i + 1, j, lps)
    else if (str.charAt(i) != substr.charAt(j) && j != 0) searchSubstring(str, substr, i, j = lps(j - 1), lps)
  }

  searchSubstring(text, pattern, 0, 0, lps)
}
