package duff.jagsql.std

import cats._
import cats.effect._
import cats.implicits._
import fs2._
import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.language.postfixOps

class PersistentTopicSpec extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  def log(s: String) = IO(println(s))

  "Single subscriber" - {

    "offer before take" in {
      val program = for {
        t      <- PersistentTopic[IO, Option[Int]]
        aStart <- t.subscribe(10).unNoneTerminate.evalTap(k => IO(println(s"first one got $k"))).compile.drain.start
        _      <- t.publish1(Some(1)) *> log(1.toString)
        _      <- t.publish1(Some(2))*> log(2.toString)
        _      <- t.publish1(Some(3))*> log(3.toString)
        bStart <- t.subscribe(10).unNoneTerminate.evalTap(k => IO(println(s"second one got $k"))).compile.drain.start
        _      <- t.publish1(None) *> log("none")
        result <- (aStart.joinWithNever *> bStart.joinWithNever).as(()).race(IO.sleep(3.second)).flatMap {
                    case Left(()) => log("Finished normally") *> IO(Left(()))
                    case Right(_) => log("ERROR") *> t.status.map(Right(_))
                  }
      } yield result

      program.unsafeRunSync() match {
        case Right(a) => println(a) 
        case _    => ()
      }

    }
  }

}
