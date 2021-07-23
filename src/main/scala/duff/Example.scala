package duff.jagsql

import cats._
import cats.effect._
import cats.effect.std.Queue
import cats.implicits._
import fs2._

import scala.concurrent.duration._

import eval._

object Example extends IOApp {

  import scala.concurrent.duration._

  def run(args: List[String]): IO[ExitCode] = {

    def log(i: String): IO[Unit] = IO(println(i))

    val count = Stream.unfold(0)(i => Some((i, i + 1))).map(_.toString).covary[IO]
    val stdin = count.take(10).noneTerminate.onFinalize(IO(println("stinconsumed")))

    def subscribe_(t: PersistentTopic[IO, Option[String]], channelName: String): Stream[IO, String] =
      Stream.eval(log(s"New subs $channelName")) *> t
        .subscribe(100)
        .unNoneTerminate
        .evalMap(i => log(s"$channelName = go $i") *> IO.pure(s"$channelName- $i"))

    def subscribe(t: Any, channelName: Any): Stream[IO, String] = stdin.unNoneTerminate

    for {
      topic <- PersistentTopic[IO, Option[String]]
      _     <- log("didier")
      emitStdin = stdin
                    .covary[IO]
                    .evalMap(i => IO(println(s"published $i")) *> topic.publish1(i))
      stuff = subscribe_(topic, "c1")
                .flatMap(x => Stream.eval(log(s"kewl $x")) *> subscribe_(topic, "c2").map(y => x -> y))
                .evalMap(a => log(a.toString))
      _     <-
        emitStdin
          .mergeHaltR(
            stuff
          )
          .compile
          .drain
    } yield ExitCode.Success

  }

}
