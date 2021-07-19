package duff
package runner

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.unsafe.IORuntime

import fs2._

import eval._

import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

// # personal objectives
// - articles, blog post
// - display customer requests, have them vote on the product
// - dataflow migration handling

class runnerSpec extends RunnerDsl {
  val sourceList = "lol" :: "http" :: Nil

  given lines: Stream[IO, String] =
    Stream.emits(sourceList).metered(1.milli)
      // .fixedDelay[IO](1.milli)
      // .zipRight(Stream.emits[IO, String](sourceList))
      // .onFinalize(IO(println("** lines done")))

  "SELECT in.col_0 FROM STDIN" evalsTo Vector(
    Row(List(("col_0", Value.VString("lol")))),
    Row(List(("col_0", Value.VString("http"))))
  )

  "SELECT in.col_0, out.col_0 AS col_1 FROM STDIN AS in JOIN (SELECT zitch.col_0 FROM STDIN AS zitch) AS out ON true" evalsTo
    sourceList
      .flatMap(i => sourceList.map(j => i -> j))
      .map { case (i, j) =>
        val l = List(("col_0", Value.VString(i)), ("col_1", Value.VString(j)))
        Row(l)
      }
      .toVector
}

object Didier extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val s = for {
      topic <- fs2.concurrent.Topic[IO, String]
      stdin = fs2.Stream.emits[IO, String](List("1", "2", "3"))
      emitStdin = stdin.evalMap(i => IO(println(s"published $i")) *> topic.publish1(i.toString))
      consumeTopic = topic.subscribe(10).evalMap(i => IO(println(s"consumed $i")))
      result = consumeTopic.concurrently(emitStdin).compile.drain
    } yield ()

    s *> IO(ExitCode.Success)
  }

}

// TODO:
// try the inlined version, one producer from stdin several consumers from streams with topic
// object Lol extends IOApp with RunnerDsl {
//   given lines: Stream[IO, String] =
//     Stream.fixedDelay[IO](10.millis).zipRight(Stream.emits[IO, String]("lol" :: "http" :: Nil))

//   def run(args: List[String]): IO[ExitCode] = {
//     // given r: IORuntime = summon[IORuntime]
//     // ("SELECT in.col_0 FROM STDIN" evalsTo_ Vector(
//     //   Row(List(("col_0", Value.VString("lol")))),
//     //   Row(List(("col_0", Value.VString("http"))))
//     // )) *>

//     ("SELECT in.col_0 FROM STDIN AS in JOIN STDIN AS out ON true" evalsTo_ Vector(
//       Row(List(("col_0", Value.VString("lol"))))
//     )) *> IO(ExitCode.Success)

//   }

// }

trait RunnerDsl extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  private def evaluate(query: String, stdIn: Stream[IO, String]): IO[Vector[Row]] = {
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
        s.compile.toVector
    }

  }

  extension (c: String) {

    def evalsTo_(expected: Vector[Row])(using stdIn: Stream[IO, String]): IO[Unit] = {
      import ast._

      evaluate(c, stdIn).map(v => require(v == expected))
    }

    def evalsTo(expected: Vector[Row])(using stdIn: Stream[IO, String]) = {
      import ast._

      c.toString in {
        assert(evaluate(c, stdIn).unsafeRunSync() == expected)
      }
    }

  }

}
