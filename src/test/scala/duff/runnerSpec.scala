package duff.jagsql
package runner

import cats._
import cats.effect._
import cats.effect.unsafe.IORuntime
import cats.implicits._
import fs2._
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

import eval._

// # personal objectives
// - articles, blog post
// - display customer requests, have them vote on the product
// - dataflow migration handling

class runnerSpec extends RunnerDsl {
  val sourceList = List("lol", "http")

  given lines: Stream[IO, String] =
    Stream.emits(sourceList)

  "SELECT in.col_0 FROM STDIN" evalsTo (1, Vector(
    Row(List(("col_0", Value.VString("lol")))),
    Row(List(("col_0", Value.VString("http"))))
  ))

  "SELECT in.col_0, out.foo AS col_1 FROM STDIN AS in JOIN (SELECT zitch.col_0 AS foo FROM STDIN AS zitch) AS out ON true" evalsTo (1000,
  sourceList
    .flatMap(i => sourceList.map(j => i -> j))
    .map { case (i, j) =>
      val l = List(("col_0", Value.VString(i)), ("col_1", Value.VString(j)))
      Row(l)
    }
    .toVector)

}

trait RunnerDsl extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  private def evaluate(query: String, stdIn: Stream[IO, String]): IO[Vector[Row]] = {
    import parser.parse
    import ast.analyzeStatement
    import planner.toStage
    import runner.toStream
    import cats.data.State
    import ast.Scope

    val stream = for {
      parsed   <- parse(query).leftMap(error => s"Error while parsing: $error")
      analyzed <- analyzeStatement(parsed)
                    .runA(Map.empty)
                    .leftMap(error => new Throwable("Error while analysing $error"))
      rootStage = toStage(analyzed)
      stream = toStream(rootStage, stdIn)
    } yield stream

    import fs2._
    import cats.effect._

    stream match {
      case Left(e)  => sys.error(e.toString)
      case Right(s) =>
        s.compile.toVector
    }

  }

  extension (c: String) {

    def evalsTo_(expected: Vector[Row])(using stdIn: Stream[IO, String]): IO[Unit] = {
      import ast._

      evaluate(c, stdIn).map(v => require(v == expected))
    }

    def evalsTo(iterations: Int, expected: Vector[Row])(using stdIn: Stream[IO, String]) = {
      import ast._

      s"$c" in {
        (0 to iterations).foreach { case i =>
          assert(evaluate(c, stdIn).unsafeRunSync() == expected, s"iteration $i failed")
        }
      }
    }

  }

}
