package duff
package grepql

import cats.effect.IO
import cats.effect.IOApp
import cats._
import cats.implicits._
import runner._

import scala.collection.immutable.ArraySeq
import cats.effect.ExitCode
import fs2._
import scala.concurrent.duration._

object Main extends IOApp {

  def run(mainArgs: List[String]): IO[ExitCode] = {
    mainArgs.toList match {
      case query :: Nil => go(query) *> IO.pure(ExitCode.Success)
      case _            =>
        // IO.raiseError(new Throwable(s"Error, you should pass only a query, got $mainArgs"))

        Stream
          .fixedDelay[IO](1.second)
          .zipRight(Stream.emits[IO, String]("lol" :: "http" :: "lala" :: "lili" :: Nil))
          .evalMap(i => IO(println(i)))
          .compile
          .drain *> IO(ExitCode.Success)
    }
  }

  private def go(query: String): IO[List[Row]] = {
    import SQLParser.parse
    import ast.analyzeStatement
    import planner.toStage
    import runner.toStream
    import cats.data.State
    import ast.Scope
    import cats._
    import cats.implicits._

    // val lines: Stream[IO, String] =
    //   Stream.fixedDelay[IO](1.second).zipRight(Stream.emits[IO, String]("lol" :: "http" :: Nil))

    val stream = for {
      parsed   <- parse(query)
      _ = println("parsing ok")
      analyzed <- analyzeStatement(parsed).runA(Map.empty).leftMap(new Throwable(_))
      _ = println("analysis ok")
      rootStage = toStage(analyzed)
      _ = println("planning ok")
      stream = toStream(rootStage)
    } yield stream

    import fs2._
    import cats.effect._

    stream match {
      case Left(e)  => sys.error(e.toString)
      case Right(s) =>
        s.evalMap(r =>
          IO {
            println(r)
            r
          }
        ).onFinalize(IO(println("done")))
          .compile
          .toList
    }

  }

}
