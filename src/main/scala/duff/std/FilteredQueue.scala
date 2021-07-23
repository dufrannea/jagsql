package duff.jagsql
package std

import cats._
import cats.effect._
import cats.effect.kernel.GenConcurrent
import cats.implicits._
import fs2._

import scala.collection.immutable.Queue

import FilteredQueue.Taker
import FilteredQueue.State

/** A Queue-like data structures that allows for a consumer to dequeue messages based on a predicate.
  *
  * Predicates are supposed to be exclusive between consumers, as messages will be sent to the first matching consumer
  * only.
  */
object FilteredQueue {

  def empty[F[_], A](using g: GenConcurrent[F, _]): F[FilteredQueue[F, A]] = emptyState.map(s => FilteredQueue[F, A](s))

  private def emptyState[F[_], A](using g: GenConcurrent[F, _]): F[Ref[F, State[F, A]]] =
    Ref[F].of(State(Queue.empty[A], Set.empty))

  private[FilteredQueue] case class Taker[F[_], A](pred: A => Boolean, deferred: Deferred[F, A])
  private[FilteredQueue] case class State[F[_], A](queue: Queue[A], takers: Set[Taker[F, A]])

}

case class FilteredQueue[F[_], A](state: Ref[F, State[F, A]])(using F: GenConcurrent[F, _]) {

  def offer(a: A): F[Unit] =
    state.update { case State(queue, takers) =>
      State(queue.enqueue(a), takers)
    } *> dequeueTakers()

  def takeIf(pred: A => Boolean): F[A] = Deferred[F, A].flatMap { taker =>
    state
      .modify { case State(queue, takers) =>
        val newTakers = takers + Taker(pred, taker)
        (State(queue, newTakers), taker.get)
      }
      .flatMap(complete => dequeueTakers() *> complete)
  }

  private def takeFirst[A](q: Queue[A], pred: A => Boolean): (Queue[A], Option[A]) = {
    def go(head: Queue[A], qq: Queue[A]): (Queue[A], Option[A]) =
      qq.dequeueOption match {
        case Some((v, nq)) if pred(v) => (head ++ nq, Some(v))
        case Some((v, nq))            => go(head.enqueue(v), nq)
        case None                     => (head, None)
      }

    go(Queue.empty[A], q)
  }

  // a successful take might enable other takes
  private def dequeueTakers() = {
    def go(q: Queue[A], t: Set[Taker[F, A]]): (Queue[A], Set[Taker[F, A]], List[(Taker[F, A], A)]) = {
      val (queue, takers, offers) = t.toList.foldLeft((q, List.empty[Taker[F, A]], List.empty[(Taker[F, A], A)])) {
        case ((queue, takerz, result), currentTaker) =>
          val (nq, maybeFound) = takeFirst(queue, currentTaker.pred)
          val newTakers =
            if (maybeFound.isEmpty)
              currentTaker :: takerz
            else takerz
          (nq, newTakers, maybeFound.map(currentTaker -> _).toList ++ result)
      }
      (queue, takers.toSet, offers)
    }

    state.modify { case State(queue, takers) =>
      val (nq, nt, toPublish) = go(queue, takers)

      (State(nq, nt), toPublish.traverse_ { case (taker, value) => taker.deferred.complete(value) })
    }.flatten

  }

  def toStream(pred: A => Boolean): Stream[F, A] =
    Stream.eval[F, A](takeIf(pred)) ++ toStream(pred)
}
