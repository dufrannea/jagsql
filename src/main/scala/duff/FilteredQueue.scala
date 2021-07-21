package duff

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
        println(s"FilteredQueue: takeIf, new Takers ${newTakers.size}, queue size ${queue.size}")
        (State(queue, newTakers), taker.get)
      }
      .flatMap(complete => dequeueTakers() *> complete)
  }

  // a successful take might enable other takes
  private def dequeueTakers() = {
    def go(q: Queue[A], takers: Set[Taker[F, A]]): (Queue[A], Set[Taker[F, A]], List[(Taker[F, A], A)]) = {
      q.headOption match {
        case Some(a) =>
          val maybeTaker = takers.find { case Taker(pred, _) => pred(a) }
          maybeTaker match {
            case None    => (q, takers, Nil)
            case Some(t) =>
              val (v, nq) = q.dequeue
              val nt = takers - t

              val (za, zi, l) = go(nq, nt)
              val dequeuedTakers = (t, v) :: l
              println(s"FilteredQueue: dequeued Takers ${dequeuedTakers.size}, takers left: ${zi.size}")
              (za, zi, dequeuedTakers)
          }
        case None    => (q, takers, Nil)
      }
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
