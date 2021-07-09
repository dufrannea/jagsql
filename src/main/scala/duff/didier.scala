package duff

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.std.Queue

import fs2._

import eval._

import scala.concurrent.duration._

// class runnerSpec extends RunnerDsl {
//   given lines: Stream[IO, String] =
//     Stream.fixedDelay[IO](10.millis).zipRight(Stream.emits[IO, String]("lol" :: "http" :: Nil))

//   "SELECT in.col_0 FROM STDIN" evalsTo Vector(
//     Row(List(("col_0", Value.VString("lol")))),
//     Row(List(("col_0", Value.VString("http"))))
//   )

//   "SELECT in.col_0 FROM STDIN AS in JOIN STDIN AS out ON true" evalsTo Vector(
//     Row(List(("col_0", Value.VString("lol"))))
//   )
// }

object Didier extends IOApp {

  import scala.concurrent.duration._
  // import fs2.concurrent.PersistentTopic

  def run(args: List[String]): IO[ExitCode] = {

    def log(i: String): IO[Unit] = IO(println(i))

    // val timer = fs2.Stream.awakeDelay[IO](1.second)

    // val stdin = timer.zipRight(Stream.constant("lol"))
    val count = Stream.unfold(0)(i => Some((i, i + 1))).map(_.toString).covary[IO]
    // Stream.constant("lol")
    val stdin = count.take(5).noneTerminate.onFinalize(IO(println("stinconsumed")))

    def subscribe(t: PersistentTopic[IO, Option[String]], channelName: String): Stream[IO, String] =
      t.subscribe(100).unNoneTerminate
      // Stream
      //   .fromQueueNoneTerminated(t)
        .evalMap(i => log(s"$channelName = go $i") *> IO.pure(s"$channelName- $i"))

    for {
      topic <- PersistentTopic[IO, Option[String]]
      _     <- log("didier")
      emitStdin = stdin
                    .covary[IO]
                    .metered(1.second)
                    .evalMap(i => IO(println(s"published $i")) *> topic.publish1(i))
      stuff = subscribe(topic, "c1")
                .flatMap(x => Stream.eval(log("kewl")) *> subscribe(topic, "c2").map(y => x -> y))
                .evalMap(a => log(a.toString))
      _     <-
        emitStdin
          .concurrently(
            stuff
          )
          .compile
          .drain
    } yield ExitCode.Success

  }

}
