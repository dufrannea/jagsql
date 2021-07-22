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
    val stdin = count.take(10).noneTerminate.onFinalize(IO(println("stinconsumed")))

    def subscribe_(t: PersistentTopic[IO, Option[String]], channelName: String): Stream[IO, String] =
      Stream.eval(log(s"New subs $channelName")) *> t
        .subscribe(100)
        .unNoneTerminate
        // Stream
        //   .fromQueueNoneTerminated(t)
        .evalMap(i => log(s"$channelName = go $i") *> IO.pure(s"$channelName- $i"))

    def subscribe(t: Any, channelName: Any): Stream[IO, String] = stdin.unNoneTerminate

    for {
      topic <- PersistentTopic[IO, Option[String]]
      _     <- log("didier")
      emitStdin = stdin
                    .covary[IO]
                    // .through(topic.publish)
                    // .metered(1.second)
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
