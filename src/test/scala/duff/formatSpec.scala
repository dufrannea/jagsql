package duff

import duff.jagsql.cst.Position

import org.scalatest.*
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class formatSpec extends AnyFreeSpec with Matchers {

  def underline(sql: String, index: Position, message: String): String = {
    val lineIndexes: Vector[(Int, Int)] = sql
      .linesIterator
      .foldLeft((0, List.empty[(Int, Int)])) { case ((from, p), current) =>
        val nextIndex = from + current.length + 1
        (nextIndex, (from -> (current.length + 1)) :: p)
      }
      ._2
      .reverse
      .toVector

    val firstIndex = lineIndexes
      .zipWithIndex
      .collectFirst { case (k, i) if k._1 <= index.from => i }
      .getOrElse(0)

    val lastIndex = lineIndexes
      .zipWithIndex
      .collectFirst { case (k, i) if (k._1 + k._2) >= index.to => i }
      .getOrElse(lineIndexes.length)

    val formattedError = (firstIndex to lastIndex).map { k =>
      val (startIndex, length) = lineIndexes(k)

      val padding =
        if (index.from >= startIndex)
          index.from - startIndex
        else 0

      val len = {
        val len_ =
          if (index.to <= Math.max(0, startIndex + length)) index.to - padding - startIndex
          else
            length - padding

        Math.max(len_ - 1, 0)
      }

      val line = "'" + sql.substring(startIndex, Math.max(0, startIndex + length - 1)) + "'"
      val underlined = "'" + (0 until padding).map(_ => " ").mkString + (0 until len).map(_ => "^").mkString + "'"

      line + "\n" + underlined
    }

    s"""
       |ERROR: $message
       |
       |${formattedError.mkString("\n")}""".stripMargin

  }

  "loll" in {
    println(
      underline(
        """0000000000100000000002
          |0000000000300000000004
          |00000000005""".stripMargin,
        Position(5, 35),
        "too bad"
      )
    )
  }

}
