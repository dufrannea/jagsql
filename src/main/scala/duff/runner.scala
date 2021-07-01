package duff
package runner

import ast.Expression
import eval._
import planner._

import cats._
import cats.effect._

import fs2.Stream
import fs2.text
import fs2.io._
import duff.ast.Projection

case class Row(colValues: List[(String, Value)])

private def evalBool(e: Expression)(context: Map[String, Value]): Boolean = {
  eval(e)(context) match {
    case Value.VBoolean(v) => v
    case _                 => sys.error("Not a boolean")
  }
}

def toStream(s: Stage): fs2.Stream[IO, Row] =
  s match {
    case Stage.ReadStdIn                                     =>
      stdinUtf8[IO](1024)
        .through(fs2.text.lines)
        .map(line => Row(List(("col_0", Value.VString(line)))))
    case Stage.Projection(projections, source)               =>
      val sourceStream = toStream(source)
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
      val sourceStream = toStream(source)
      sourceStream.filter { case Row(inputColValues) =>
        val lookup = inputColValues.toMap
        evalBool(predicate)(lookup)
      }
    case Stage.Join(leftSource, rightSource, maybePredicate) =>
      val leftStream = toStream(leftSource)
      val rightStream = toStream(rightSource)

      val crossJoin = for {
        leftRow  <- leftStream
        rightRow <- rightStream
      } yield (Row(leftRow.colValues ++ rightRow.colValues))

      maybePredicate match {
        case None            => crossJoin
        case Some(predicate) =>
          val filtered = crossJoin.filter { case Row(colValues) =>
            val values = colValues.toMap
            evalBool(predicate)(values)
          }
          filtered
      }
  }
