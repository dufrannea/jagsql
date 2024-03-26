package duff.jagsql
package repl

import duff.jagsql.ast.validation.{analyzeStatement, Scope, TrackedError}
import duff.jagsql.cst.Statement.SelectStatement
import duff.jagsql.parser.parse
import duff.jagsql.planner.toStage
import duff.jagsql.runner.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.*

import com.monovore.decline.{Command, CommandApp, Opts}
import org.jline.reader.{LineReader, LineReaderBuilder}
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder

object Main extends IOApp {

  private val query = Opts.option[String]("query", "Input query").orNone
  private val file = Opts.option[String]("file", "Input file").orNone

  private val command = Command("jag", "Execute a sql query from the command line.") {
    (query, file).tupled
  }

  private val reader = {
    val terminal = TerminalBuilder.terminal()

    val parser = new DefaultParser()

    val history: DefaultHistory = new DefaultHistory()

    val lineReader = LineReaderBuilder
      .builder()
      .terminal(terminal)
      .history(history)
      .appName("jagsql")
      .build()

    lineReader.setOpt(LineReader.Option.CASE_INSENSITIVE_SEARCH)
    lineReader.setVariable(LineReader.HISTORY_FILE, Paths.get(System.getProperty("user.home"), ".jagsql"))

    lineReader
  }

  def run(mainArgs: List[String]): IO[ExitCode] =
    command.parse(mainArgs) match {
      case Left(help)                     =>
        IO(System.err.println(help)) *> IO.pure(ExitCode.Error)
      case Right((maybeQuery, maybeFile)) =>
        val query = maybeFile
          .map(file => new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8))
          .orElse(maybeQuery)

        query match {
          case Some(query) =>
            go(query) *> IO(ExitCode.Success)
          case None        =>
            loop.as(ExitCode.Success)
        }

    }

  def loop: IO[Unit] = {
    def loop0: IO[Unit] =
      for {
        input <- IO(reader.readLine("> "))
        _     <- go(input).recoverWith {
                   case ValidationError(e) =>
                     IO.println("Validation error") *> IO.println(e)
                   case ParsingError(e)    =>
                     IO.println("Parsing error") *> IO.println(e)
                 }
        _     <- loop
      } yield ()

    loop0.recoverWith { case e: java.io.EOFException =>
      IO.println("End of input")
    }
  }

  case class ValidationError(e: TrackedError) extends Throwable
  case class ParsingError(e: String) extends Throwable

  private def go(query: String): IO[Unit] = {

    val stream = for {
      parsed   <- parse(query).leftMap(e => ParsingError(e))
      analyzed <- analyzeStatement(parsed).runA(Map.empty).leftMap(k => ValidationError(k))
      rootStage = toStage(analyzed)
      stream = toStream(rootStage)
    } yield prettyPrintResultSet(analyzed, stream)

    stream match {
      case Left(e)  => IO.raiseError(e)
      case Right(s) => s
    }

  }

}

def prettyPrintResultSet(
  selectStatement: ast.Statement.SelectStatement,
  resultSet: fs2.Stream[IO, Row]
): IO[Unit] = {
  import eval.SqlShow.sqlPrint
  import eval.given

  def formatLine(values: List[String]) =
    values
      .map(value => f"$value%10s")
      .mkString(" | ")

  val colNames: String =
    formatLine(
      selectStatement
        .tableType
        .cols
        .toNel
        .map(_._1)
        .toList
    )

  val printHeader = IO.println(colNames) *> IO.println(colNames.map(k => "_").mkString)
  val consumeAndPrintLines = resultSet
    .evalMap { row =>
      IO.println(formatLine(row.colValues.map(_._2.sqlPrint)))
    }
    .compile
    .drain

  printHeader *> consumeAndPrintLines
}
