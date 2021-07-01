package duff
package grepql

import cats.effect.IO
import cats.effect.IOApp
import cats._
import cats.implicits._

import scala.collection.immutable.ArraySeq
import cats.effect.ExitCode

object Main extends IOApp {

  def run(mainArgs: List[String]): IO[ExitCode] = {
    mainArgs.toList match {
      case query :: Nil => go(query) *> IO.pure(ExitCode.Success)
      case _            => IO.raiseError(new Throwable(s"Error, you should pass only a query, got $mainArgs"))
    }
  }

  private def go(query: String): IO[Unit] = {
    import SQLParser.parse
    import ast.analyzeStatement
    import planner.toStage
    import runner.toStream
    import cats.data.State
    import ast.Scope

    val stream = for {
      parsed   <- parse(query)
      analyzed <- analyzeStatement(parsed).runA(Map.empty).leftMap(new Throwable(_))
      rootStage = toStage(analyzed)
      stream = toStream(rootStage)
    } yield stream

    import fs2._
    import cats.effect._

    stream match {
      case Left(e)  => sys.error(e.toString)
      case Right(s) =>
        s.evalMap(r => IO(println(r.colValues.map(_._2).toString))).compile.drain
    }

  }

}
