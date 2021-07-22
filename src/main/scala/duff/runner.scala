package duff.jagsql
package runner

import ast.Expression
import ast.Projection
import eval._
import planner._

import cats._
import cats.effect._

import fs2.Stream
import fs2.text
import fs2.io._

case class Row(colValues: List[(String, Value)])

private def unsafeEvalBool(e: Expression)(context: Map[String, Value]): Boolean = {
  eval(e)(context) match {
    case Value.VBoolean(v) => v
    case _                 => sys.error("Not a boolean")
  }
}

val defaultStdInLines = stdinUtf8[IO](1024)
  .through(fs2.text.lines)
  .dropRight(1)

def toStream(s: Stage, stdInLines: Stream[IO, String] = defaultStdInLines): fs2.Stream[IO, Row] = {
  def publishStdIn(topic: PersistentTopic[IO, Option[Row]]) =
    stdInLines
      .map(line => Row(List(("col_0", Value.VString(line)))))
      .noneTerminate
      .through(
        topic.publish
      )

  def toStream0(s: Stage, topic: PersistentTopic[IO, Option[Row]], name: Option[String] = None): fs2.Stream[IO, Row] =
    s match {
      case Stage.ReadStdIn                                     =>
        topic
          .subscribe(10)
          .evalMap(k =>
            IO {
              k
            }
          )
          .unNoneTerminate
      case Stage.Projection(projections, source)               =>
        val sourceStream = toStream0(source, topic)
        sourceStream.map { case Row(cols) =>
          val lookup = cols.toMap
          val colValues = projections
            .zipWithIndex
            .map { case (Projection(expression, maybeAlias), index) =>
              (maybeAlias.getOrElse(s"col_$index"), eval(expression)(lookup))
            }
          Row(colValues.toList)
        }
      case Stage.Filter(predicate, source)                     =>
        val sourceStream = toStream0(source, topic)
        sourceStream.filter { case Row(inputColValues) =>
          val lookup = inputColValues.toMap
          unsafeEvalBool(predicate)(lookup)
        }
      case Stage.Join(leftSource, rightSource, maybePredicate) =>
        val leftStream = toStream0(leftSource, topic, Some("left"))
        val rightStream = toStream0(rightSource, topic, Some("right"))

        // one option would be to fully store the right stream
        // in memory, and reiterate on it. Doing so would prevent
        // from displaying values as soon as possible, so an alternative
        // approach using a PersistentTopic is used here.
        val crossJoin = for {
          leftRow  <- leftStream
          rightRow <- rightStream
          allCols = leftRow.colValues ++ rightRow.colValues
        } yield Row(allCols)

        maybePredicate match {
          case None            => crossJoin
          case Some(predicate) =>
            val filtered = crossJoin.filter { case Row(colValues) =>
              val values = colValues.toMap
              unsafeEvalBool(predicate)(values)
            }
            filtered
        }
    }

  val result = for {
    topic  <- Stream.eval(PersistentTopic[IO, Option[Row]])
    stream <- publishStdIn(topic)
                .drain
                .mergeHaltR(toStream0(s, topic))
  } yield stream

  result
}
