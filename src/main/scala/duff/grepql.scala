package duff.jagsql

import duff.jagsql.ast.{analyzeStatement, Scope}
import duff.jagsql.cst.Indexed
import duff.jagsql.parser.parse
import duff.jagsql.planner.toStage
import duff.jagsql.runner.*

import scala.collection.immutable.ArraySeq
import scala.concurrent.duration.*

import cats.*
import cats.data.State
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.*
import fs2.*

import com.monovore.decline.{Command, CommandApp, Opts}

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
