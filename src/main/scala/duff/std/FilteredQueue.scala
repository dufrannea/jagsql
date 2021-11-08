package duff.jagsql
package std

import duff.jagsql.std.FilteredQueue.State
import duff.jagsql.std.FilteredQueue.Taker

import scala.collection.immutable.Queue

import cats._
import cats.effect._
import cats.effect.kernel.GenConcurrent
import cats.implicits._
import fs2._

/** A Queue-like data structures that allows for a consumer to dequeue messages based on a predicate.
  *
  * Predicates are supposed to be exclusive between consumers, as messages will be sent to the first matching consumer
  * only.
  */
object FilteredQueue {

  def empty[F[_], A](using g: GenConcurrent[F, _]): F[FilteredQueue[F, A]] = emptyState.map(s => FilteredQueue[F, A](s))

  private def emptyState[F[_], A](using g: GenConcurrent[F, _]): F[Ref[F, State[F, A]]] =
    Ref[F].of(State(Queue.empty[A], Set.empty))

  private[std] case class Taker[F[_], A](pred: A => Boolean, deferred: Deferred[F, A]) {
    override def toString = s"Taker($pred)"
  }

  case class State[F[_], A](queue: Queue[A], takers: Set[Taker[F, A]])

  private[std] def takeFirst[A](q: Queue[A], pred: A => Boolean): (Queue[A], Option[A]) = {
    def go(head: Queue[A], qq: Queue[A]): (Queue[A], Option[A]) =
      qq.dequeueOption match {
        case Some((v, nq)) if pred(v) => (head ++ nq, Some(v))
        case Some((v, nq))            => go(head.enqueue(v), nq)
        case None                     => (head, None)
      }

    go(Queue.empty[A], q)
  }

  private[std] def dequeueTakers0[F[_], A](
    messageQueue: Queue[A],
    takers: Set[Taker[F, A]]
  ): (Queue[A], Set[Taker[F, A]], List[(Taker[F, A], A)]) = {
    val (nQueue, nTakers, offers) =
      takers.toList.foldLeft((messageQueue, List.empty[Taker[F, A]], List.empty[(Taker[F, A], A)])) {
        case ((queueAcc, takersAcc, result), currentTaker) =>
          val (nq, maybeFound) = FilteredQueue.takeFirst(queueAcc, currentTaker.pred)
          println(s"$nq, $maybeFound")
          val newTakers =
            if (maybeFound.isEmpty)
              currentTaker :: takersAcc
            else takersAcc
          (nq, newTakers, maybeFound.map(currentTaker -> _).toList ++ result)
      }
    (nQueue, nTakers.toSet, offers)
  }

}

case class FilteredQueue[F[_], A](state: Ref[F, State[F, A]])(using F: GenConcurrent[F, _]) {

  def status: F[State[F, A]] = state.get

  def logStatus(when: String): F[Unit] = status >>= (status => F.pure(println(s"$when: $status")))

  def offer(a: A): F[Unit] =
    state.update { case State(queue, takers) =>
      State(queue.enqueue(a), takers)
    } *> logStatus(s"In offer $a") *> dequeueTakers(s"offer $a")

  def takeIf(pred: A => Boolean, debug: Any = ""): F[A] = Deferred[F, A].flatMap { taker =>
    state
      .modify { case State(queue, takers) =>
        val newTakers = takers + Taker(pred, taker)
        (State(queue, newTakers), taker.get)
      }
      .flatMap(complete => logStatus(s"In takeIf $debug") *> dequeueTakers(s"takeIf $debug") *> complete)
  }

  def toStream(pred: A => Boolean, debug: Any = ""): Stream[F, A] =
    Stream.eval[F, A](takeIf(pred, debug)) ++ toStream(pred, debug)

  private[std] def dequeueTakers(debug: String = "") = {
    var count = 0
    state.modify { case State(queue, takers) =>
      count += 1
      val (nq, nt, toPublish) = FilteredQueue.dequeueTakers0(queue, takers)

      println(s"In dequeue takers ($debug)[$count], $nq, $nt=> to publish: $toPublish")

      (State(nq, nt), toPublish.traverse_ { case (taker, value) => taker.deferred.complete(value) })
    }.flatten

  }

}
