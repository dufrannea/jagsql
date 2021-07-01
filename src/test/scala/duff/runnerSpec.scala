package duff
package runner

import cats._
import cats.implicits._
import cats.effect._

import fs2._

import eval._

import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class runnerSpec extends RunnerDsl {
  given lines: Stream[IO, String] =
      Stream.fixedDelay[IO](1.second).zipRight(Stream.emits[IO, String]("lol" :: Nil))

  "SELECT in.col_0 FROM STDIN" evalsTo Vector(Row(List(("col_0", Value.VString("lol")))))
}

trait RunnerDsl extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  private def evaluate(query: String, stdIn: Stream[IO, String]): Vector[Row] = {
    import SQLParser.parse
    import ast.analyzeStatement
    import planner.toStage
    import runner.toStream
    import cats.data.State
    import ast.Scope

    val stream = for {
      parsed   <- parse(query)
      _ = println("parsing ok")
      analyzed <- analyzeStatement(parsed)
                    .runA(Map.empty)
                    .leftMap(new Throwable(_))
      _ = println("analysis ok")
      rootStage = toStage(analyzed)
      _ = println(rootStage)
      _ = println("planning ok")
      stream = toStream(rootStage, stdIn)
    } yield stream

    import fs2._
    import cats.effect._

    stream match {
      case Left(e)  => sys.error(e.toString)
      case Right(s) =>
        s.compile.toVector.unsafeRunSync()
    }

  }

  extension (c: String) {

    def evalsTo(expected: Vector[Row])(using stdIn: Stream[IO, String]): Unit = {
      import ast._

      c.toString in {
        assert(evaluate(c, stdIn) == expected)
      }
    }
  }

}
