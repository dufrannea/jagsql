package duff.jagsql.std

import java.nio.file.DirectoryStream.Filter

import scala.concurrent.duration.*
import scala.language.postfixOps

import cats.*
import cats.effect.*
import cats.implicits.*
import fs2.*

import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

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
        p.unsafeRunTimed(5.second).isEmpty
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

  }

  import scala.collection.immutable.Queue
  "takeFirst" - {

    "should return same queue when no match" in {
      val q = Queue.empty[Int].enqueue(1).enqueue(2)
      assert(FilteredQueue.takeFirst(q, _ == 3) == (q, None))
    }

    "should Find if in first position" in {

      val q = Queue(1, 2)
      assert(FilteredQueue.takeFirst(q, _ == 2) == (Queue(1), Some(2)))
    }

    "should Find if in second position" in {

      val q = Queue(1, 2)
      assert(FilteredQueue.takeFirst(q, _ == 1) == (Queue(2), Some(1)))
    }

    "should return only one match" in {

      val q = Queue(1, 2)
      assert(FilteredQueue.takeFirst(q, _ => true) == (Queue(2), Some(1)))
    }
  }

}
