package duff.jagsql
package runner

import duff.jagsql.ast.validation.*
import duff.jagsql.eval.*

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.concurrent.duration.*

import cats.*
import cats.effect.*
import cats.effect.unsafe.IORuntime
import cats.implicits.*
import fs2.*

import org.scalatest.*
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class runnerSpec extends RunnerDsl {
  val sourceList = List("lol", "http")

  given lines: Stream[IO, String] =
    Stream.emits(sourceList)

  val lines2: Stream[IO, String] =
    Stream.emits(List("lol", "http", "lol"))

  val createdFilePath = {
    val file = Files.createTempFile("runnerSpecTest", "foo")
    Files.write(file, "foo".getBytes(StandardCharsets.UTF_8)).toAbsolutePath.toString
  }

  "SELECT in.col_0 FROM STDIN" evalsTo Vector(
    Row(List(("col_0", Value.VString("lol")))),
    Row(List(("col_0", Value.VString("http"))))
  )

  "SELECT in.col_0, out.foo AS col_1 FROM STDIN AS in JOIN (SELECT zitch.col_0 AS foo FROM STDIN AS zitch) AS out ON true" evalsTo sourceList
    .flatMap(i => sourceList.map(j => i -> j))
    .map { case (i, j) =>
      val l = List(("col_0", Value.VString(i)), ("col_1", Value.VString(j)))
      Row(l)
    }
    .toVector

  s"SELECT in.col_0 FROM FILE('$createdFilePath') AS in" evalsTo {
    val l = List(("col_0", Value.VString("foo")))
    Vector(Row(l))
  }

  "SELECT in.col_0 FROM STDIN AS in WHERE in.col_0 = 'lol'" evalsTo Vector(
    Row(List(("col_0", Value.VString("lol"))))
  )

  "SELECT in.col_0 FROM STDIN AS in GROUP BY in.col_0".evalsTo(
    Vector(
      Row(List(("col_0", Value.VString("lol")))),
      Row(List(("col_0", Value.VString("http"))))
    )
  )(
    using lines2
  )

  "SELECT max(to_int(on.col_0)) FROM STDIN AS on GROUP BY on.col_0".evalsTo(
    Vector(
      Row(List(("col_0", Value.VNumber(2)))),
      Row(List(("col_0", Value.VNumber(1))))
    )
  )(
    using Stream.emits(List("1", "2"))
  )

  "SELECT on.col_0 FROM DUAL AS on".evalsTo(
    Vector(
      Row(List(("col_0", Value.VString(""))))
    )
  )(
    using Stream.emits(List("1", "2"))
  )

}

trait RunnerDsl extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  private def evaluate(query: String, stdIn: Stream[IO, String]): IO[Vector[Row]] = {
    import ast.validation.{Scope, analyzeStatement}
    import cats.data.State
    import parser.parse
    import planner.toStage
    import runner.toStream

    val stream = for {
      parsed   <- parse(query).leftMap(error => s"Error while parsing: $error")
      analyzed <- analyzeStatement(parsed)
                    .runA(Map.empty)
                    .leftMap(error => new Throwable(s"Error while analysing $error"))
      rootStage = toStage(analyzed)
      stream = toStream(rootStage, stdIn)
    } yield stream

    import cats.effect.*
    import fs2.*

    stream match {
      case Left(e)  => sys.error(e.toString)
      case Right(s) =>
        s.compile.toVector
    }

  }

  extension (c: String) {

    def evalsTo_(expected: Vector[Row])(using stdIn: Stream[IO, String]): IO[Unit] = {
      import ast.*

      evaluate(c, stdIn).map(v => require(v == expected))
    }

    def evalsTo(expected: Vector[Row])(using stdIn: Stream[IO, String]): Unit =
      evalsTo(1, expected)

    def evalsTo(iterations: Int, expected: Vector[Row])(using stdIn: Stream[IO, String]) = {
      import ast.*

      s"$c" in {
        (0 to iterations).foreach { case i =>
          assert(evaluate(c, stdIn).unsafeRunSync() == expected, s"iteration $i failed")
        }
      }
    }

  }

}
