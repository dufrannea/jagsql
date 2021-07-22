package duff.jagsql

import cats._
import cats.implicits._
import cats.effect._

import cats.effect.kernel.GenConcurrent
import scala.collection.immutable.Queue
import fs2._

object FilteredQueue {
  private def emptyState[F[_], A](using g: GenConcurrent[F, _]): F[Ref[F, State[F, A]]] =
    Ref[F].of(State(Queue.empty[A], Set.empty))

  def empty[F[_], A](using g: GenConcurrent[F, _]): F[FilteredQueue[F, A]] = emptyState.map(s => FilteredQueue[F, A](s))
}

case class Taker[F[_], A](pred: A => Boolean, deferred: Deferred[F, A])
case class State[F[_], A](queue: Queue[A], takers: Set[Taker[F, A]])

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

  type State = Ref[F, Queue[A]]

  def toStream(pred: A => Boolean): Stream[F, A] =
    Stream.eval[F, A](takeIf(pred)) ++ toStream(pred)
}
