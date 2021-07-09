package duff

import cats._
import cats.effect._
import cats.implicits._

import fs2._

object Test extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    val r = Stream.emit("super\nles\namis\n").through(fs2.text.lines).toList
    println(r)
    IO.pure(ExitCode.Success)

}
