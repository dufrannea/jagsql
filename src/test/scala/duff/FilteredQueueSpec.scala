package duff.jagsql

import cats._
import cats.implicits._
import cats.effect._

import fs2._

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest

import scala.language.postfixOps
import scala.concurrent.duration._

class FilteredQueueSpec extends AnyFreeSpec with Matchers {
  import cats.effect.unsafe.implicits.global

  "Single subscriber" - {

    "offer before take" in {
      val p = for {
        q   <- FilteredQueue.empty[IO, String]
        _   <- q.offer("foo")
        foo <- q.takeIf(_ == "foo")
      } yield foo

      val foo = p.unsafeRunSync()
      assert(foo == "foo")
    }

    "block if take before offer" in {
      val p = for {
        q   <- FilteredQueue.empty[IO, String]
        foo <- q.takeIf(_ == "foo")
        _   <- q.offer("foo")
      } yield foo

      assert {
        p.unsafeRunTimed(5.second) == None
      }
    }

    "succeeds if take async before offer" in {
      val p = for {
        q        <- FilteredQueue.empty[IO, String]
        fooFiber <- q.takeIf(_ == "foo").start
        _        <- q.offer("foo")
        foo      <- fooFiber.joinWithNever
      } yield foo

      val foo = p.unsafeRunSync()
      assert(foo == "foo")
    }
  }

  "Multiple takers" - {

    "Order of takers does not matter" in {
      val p = for {
        q        <- FilteredQueue.empty[IO, String]
        _        <- q.offer("foo")
        _        <- q.offer("bar")
        // register takers in reverse order
        // of messages, it should not matter
        barFiber <- q.takeIf(_ == "bar").start
        fooFiber <- q.takeIf(_ == "foo").start
        bar      <- barFiber.joinWithNever
        foo      <- fooFiber.joinWithNever
      } yield foo :: bar :: Nil

      p.unsafeRunTimed(5.second) match {
        case Some(l) =>
          assert(l.contains("foo"))
          assert(l.contains("bar"))
          assert(l.size == 2)
        case None    => fail("Should not timeout")
      }
    }

  }

  "ToStream" in {
    val extracted = for {
      queue  <- FilteredQueue.empty[IO, Option[Int]]
      values <- queue
                  .toStream(_ => true)
                  .unNoneTerminate
                  .concurrently(
                    Stream
                      .emits(1 to 10)
                      .covary[IO]
                      .noneTerminate
                      .evalMap(queue.offer)
                  )
                  .compile
                  .toList
    } yield values

    println(extracted.unsafeRunSync())

  }

}
