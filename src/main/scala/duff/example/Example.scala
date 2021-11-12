package duff.jagsql
package example

import duff.jagsql.std.PersistentTopic

import scala.concurrent.duration.*

import cats.*
import cats.effect.*
import cats.effect.std.Queue
import cats.implicits.*
import fs2.*

object Example extends IOApp {

  import scala.concurrent.duration.*

  def run(args: List[String]) =
    (1 to 100)
      .toList
      .traverse_(_ =>
        IO(println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>")) *> run0(args).flatMap {
          case Left(_)  => IO.unit
          case Right(x) => IO.raiseError(new Throwable(s"************************************* $x"))
        }
      )
      .as(ExitCode.Success)

  def run0(args: List[String]): IO[Either[Unit, Unit]] = {

    def log(i: String): IO[Unit] = IO(println(i))

    val count = Stream.unfold(0)(i => Some((i, i + 1))).map(_.toString).covary[IO]
    val stdin = count.take(1).noneTerminate.onFinalize(IO(println("stinconsumed")))

    def subscribe_(t: PersistentTopic[IO, Option[String]], channelName: String): Stream[IO, String] =
      Stream.eval(log(s"New subs $channelName")) *> t
        .subscribe(100)
        .unNoneTerminate
        .evalMap(i => log(s"$channelName dequeued $i") *> IO.pure(s"$channelName- $i"))

    def subscribe(t: Any, channelName: Any): Stream[IO, String] = stdin.unNoneTerminate

    for {
      topic  <- PersistentTopic[IO, Option[String]]
      emitStdin = stdin
                    .covary[IO]
                    .evalMap(i => IO(println(s"published $i")) *> topic.publish1(i))
      stuff = subscribe_(topic, "c1")
                .flatMap(x => subscribe_(topic, "c2").map(y => x -> y))
      // .evalMap(a => log(a.toString))
      result <-
        emitStdin
          .mergeHaltR(
            stuff
          )
          .compile
          .drain
          .race(IO.sleep(10.second) *> topic.status)
    } yield result

  }

}
