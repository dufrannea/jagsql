package duff.jagsql
package std

import scala.util.Try
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

case class RegexWrapper private[RegexWrapper] (pattern: String) {
  lazy val regex = new Regex(pattern)
  lazy val groups: Seq[(Int, Option[String])] = RegexWrapper.getGroups(regex)
  lazy val groupCount = groups.size
}

object RegexWrapper {

  def build(pattern: String): Try[RegexWrapper] =
    Try {
      new Regex(pattern)
    }.map(_ => RegexWrapper(pattern))

  private[RegexWrapper] def getGroups(inputRegex: Regex): List[(Int, Option[String])] = {
    val input = inputRegex.regex

    val s = scala.collection.mutable.Stack.empty[(Int, Boolean, Option[String])]
    var inSquareBrackets = false

    var groupCount: Int = 0
    val result = collection.mutable.ListBuffer.empty[(Int, Option[String])]

    def getName(start: Int): Option[(Int, String)] =
      if (start < input.length + 2 && input(start + 1) == '?' && input(start + 2) == '<') {
        var current = start + 3
        val groupName = new StringBuilder()

        // regex names cannot be escaped
        breakable {
          while (true) {
            if (current == input.length) {
              sys.error("Should not happen on a valid regex")
            }
            val currentChar = input(current)
            // we found a name
            if (currentChar == '>') {
              break
            }
            groupName += currentChar
            current += 1

          }
        }

        Some((current, groupName.toString()))

      } else {
        None
      }

    var i = 0
    while (i < input.length) {

      val k = input(i)
      k match {
        case '(' if !inSquareBrackets =>
          val nonCapturing = i <= input.length + 2 && input(i + 1) == '?' && input(i + 2) == ':'
          // maybe named group
          val maybeName = if (nonCapturing) {
            None
          } else {
            getName(i) match {
              case Some((nextIndex, name)) =>
                i = nextIndex
                Some(name)
              case None                    => None
            }
          }
          s.push((groupCount, nonCapturing, maybeName))
          groupCount += 1
        case ')' if !inSquareBrackets =>
          val (groupNumber, nonCapturing, maybeName) = s.pop()
          if (!nonCapturing) {
            result.prepend((groupNumber, maybeName))
          }
        case '['                      =>
          require(!inSquareBrackets, "Should not be in square brackets")
          inSquareBrackets = true
        case ']'                      =>
          require(inSquareBrackets, "Should be in square brackets")
          inSquareBrackets = false
        case '\\'                     =>
          i += 1
        case _                        => ()
      }
      i += 1
    }
    assert(s.isEmpty, "Stack should be empty at the end of the algo")

    result.sortBy(_._1).toList
  }

}
