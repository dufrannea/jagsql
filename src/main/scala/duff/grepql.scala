package duff.jagsql

import cats._
import cats.data.State
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.implicits._
import com.monovore.decline.Command
import com.monovore.decline.CommandApp
import com.monovore.decline.Opts
import fs2._

import scala.collection.immutable.ArraySeq
import scala.concurrent.duration._

import SQLParser.parse
import ast.analyzeStatement
import ast.Scope
import planner.toStage
import runner.toStream
import runner._

object Main extends IOApp {

  private val options = Opts.option[String]("query", "Input query")

  private val command = Command("jag", "Execute a sql query from the command line.") {
    options
  }

  def run(mainArgs: List[String]): IO[ExitCode] =
    command.parse(mainArgs) match {
      case Left(help)   =>
        IO(System.err.println(help)) *> IO.pure(ExitCode.Error)
      case Right(query) =>
        go(query) *> IO(ExitCode.Success)
    }

  private def go(query: String): IO[List[Row]] = {

    val stream = for {
      parsed   <- parse(query)
      _ = println("parsing ok")
      analyzed <- analyzeStatement(parsed).runA(Map.empty).leftMap(new Throwable(_))
      _ = println("analysis ok")
      rootStage = toStage(analyzed)
      _ = println("planning ok")
      stream = toStream(rootStage)
    } yield stream

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
