package duff.jagsql
package runner

import duff.jagsql.ast.Expression
import duff.jagsql.ast.Projection
import duff.jagsql.eval.*
import duff.jagsql.eval.Value.VString
import duff.jagsql.planner.*
import duff.jagsql.std.PersistentTopic

import java.io.FileInputStream

import cats.*
import cats.data.Binested
import cats.data.EitherK
import cats.data.Nested
import cats.effect.*
import cats.implicits.*
import fs2.Stream
import fs2.io.*
import fs2.text

// TODO: would be nice to remove the string and only
// keep the values
case class Row(colValues: List[(String, Value)])

private def unsafeEvalBool(e: Expression)(context: Map[String, Value]): Boolean =
  eval(e)(context) match {
    case Value.VBoolean(v) => v
    case _                 => sys.error("Not a boolean")
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

  def toStream0(s: Stage, name: Option[String] = None): MaybeNeedsTopic[Stream[IO, Row]] =
    s match {
      case Stage.ReadStdIn                                     =>
        Left(topic =>
          topic
            .subscribe(10)
            .unNoneTerminate
        )
      case Stage.ReadDual                                      =>
        Right(
          Stream.emits(List(Row(("col_0" -> Value.VString("")) :: Nil))).covary[IO]
        )
      case Stage.ReadFile(file)                                =>
        Right(
          readInputStream[IO](IO(new FileInputStream(file)), 1024)
            .through(fs2.text.utf8.decode)
            .through(fs2.text.lines)
            .dropLastIf(_.isEmpty)
            .map(line => Row(List(("col_0", Value.VString(line)))))
        )
      case Stage.RunCommand(command)                           =>
        import sys.process.*

        val result = command.!!
        val stream = Stream
          .emits(result.linesIterator.toList)
          .covary[IO]
          .map(line => Row(List(("col_0", Value.VString(line)))))

        Right(stream)
      case Stage.Projection(projections, source)               =>
        val sourceStream = toStream0(source)

        // Need to explicit the types as we do not want
        // the function instance of Either to be picked up
        Nested[MaybeNeedsTopic, [X] =>> Stream[IO, X], Row](sourceStream).map { case Row(cols) =>
          val lookup = cols.toMap
          val colValues = projections
            .zipWithIndex
            .map { case (Projection(expression, maybeAlias), index) =>
              (maybeAlias.getOrElse(s"col_$index"), eval(expression)(lookup))
            }

          Row(colValues.toList)
        }.value
      case Stage.Filter(predicate, source)                     =>
        val sourceStream = toStream0(source)

        Functor[MaybeNeedsTopic].map(sourceStream)(_.filter { case Row(inputColValues) =>
          val lookup = inputColValues.toMap
          unsafeEvalBool(predicate)(lookup)
        })
      case Stage.Join(leftSource, rightSource, maybePredicate) =>
        val leftStream = toStream0(leftSource, Some("left"))
        val rightStream = toStream0(rightSource, Some("right"))

        // one option would be to fully store the right stream
        // in memory, and reiterate on it. Doing so would prevent
        // from displaying values as soon as possible, so an alternative
        // approach using a PersistentTopic is used here.
        (leftStream, rightStream).mapN { case (ls, rs) =>
          val crossJoin = for {
            leftRow  <- ls
            rightRow <- rs
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
      case Stage.GroupBy(groupings, aggregations, source)      =>
        Functor[MaybeNeedsTopic].map(toStream0(source)) { s =>
          val groupLines = s.compile.toList.map { (list: List[Row]) =>
            val aggregatedRows = list
              .groupMap { row =>
                Row(groupings.map { case (grouping, groupingColName) =>
                  groupingColName -> eval(grouping)(row.colValues.toMap)
                }.toList)
              }(identity)
              .map { case (keysRow, aggregatedRows) =>
                val aggregatedValues = aggregations.map { case (aggExpression, aggName) =>
                  val ast.ExpressionF.FunctionCallExpression(aggregateFunction, args, _) =
                    aggExpression.unfix.asInstanceOf[ast.ExpressionF.FunctionCallExpression[ast.Expression]]
                  val argExpression = args.head
                  val toBeAggregated: List[Value] =
                    aggregatedRows.map(_.colValues.toMap ++ keysRow.colValues.toMap).map { scope =>
                      eval(argExpression)(scope)
                    }
                  val aggregateValue = aggregateFunction.asInstanceOf[ast.AggregateFunction].run(toBeAggregated)
                  aggName -> aggregateValue
                }

                Row(keysRow.colValues ++ aggregatedValues)
              }

            aggregatedRows.toSeq
          }

          Stream.evalSeq(groupLines)

        }

    }

  toStream0(s) match {
    case Left(needsTopic)     =>
      for {
        topic  <- Stream.eval(PersistentTopic[IO, Option[Row]])
        stream <- publishStdIn(topic)
                    .drain
                    .mergeHaltR(needsTopic(topic))
      } yield stream
    case Right(noTopicNeeded) =>
      noTopicNeeded
  }
}

type StdInTopic = PersistentTopic[IO, Option[Row]]

type MaybeNeedsTopic[K] = Either[StdInTopic => K, Id[K]]

given maybeNeedsTopicApply: Apply[MaybeNeedsTopic] = new MaybeNeedsTopicApply {}

trait MaybeNeedsTopicApply extends Apply[MaybeNeedsTopic] {
  override def map[A, B](fa: MaybeNeedsTopic[A])(f: A => B) =
    EitherK(fa).map(f).run

  override def ap[A, B](f: MaybeNeedsTopic[A => B])(fa: MaybeNeedsTopic[A]): MaybeNeedsTopic[B] =
    (f, fa) match {
      case (Right(ff), Right(a)) => Right(ff(a))
      case (Left(ff), Right(a))  => Left(topic => ff(topic)(a))
      case (Right(ff), Left(a))  => Left(topic => ff(a(topic)))
      case (Left(ff), Left(a))   => Left(topic => ff(topic)(a(topic)))
    }

}
