package duff.jagsql
package std

import scala.collection.immutable.LongMap

import cats.effect._
import cats.effect.implicits._
import cats.syntax.all._
import fs2._
import fs2.concurrent._

abstract class PersistentTopic[F[_], A] { self =>

  def status = IO.unit

  def publish: Pipe[F, A, Nothing]

  def publish1(a: A): F[Either[PersistentTopic.Closed, Unit]]

  def subscribe(maxQueued: Int): Stream[F, A]

  def subscribeAwait(maxQueued: Int): Resource[F, Stream[F, A]]

  def subscribers: Stream[F, Int]

  def close: F[Either[PersistentTopic.Closed, Unit]]

  /** Returns true if this PersistentTopic is closed */
  def isClosed: F[Boolean]

  /** Semantically blocks until the PersistentTopic gets closed. */
  def closed: F[Unit]

}

object PersistentTopic {
  type Closed = Closed.type
  object Closed

  import scala.collection.immutable.Queue

  case class ChannelState[F[_], A](channel: Channel[F, A], ready: Deferred[F, Unit])
  case class State[F[_], A](chans: LongMap[ChannelState[F, A]], counter: Long, backlog: Queue[A])

  /** Constructs a PersistentTopic */
  def apply[F[_], A](implicit F: Concurrent[F]): F[PersistentTopic[F, A]] =
    (
      F.ref(State[F, A](LongMap.empty[ChannelState[F, A]], 1L, Queue.empty)),
      SignallingRef[F, Int](0),
      F.deferred[Unit]
    ).mapN { case (state, subscriberCount, signalClosure) =>
      new PersistentTopic[F, A] {

        def foreach[B](lm: LongMap[B])(f: B => F[Unit]) =
          lm.foldLeft(F.unit) { case (op, (_, b)) => op >> f(b) }

        def publish1(a: A): F[Either[PersistentTopic.Closed, Unit]] =
          signalClosure.tryGet.flatMap {
            case Some(_) => PersistentTopic.closed.pure[F]
            case None    =>
              state
                .updateAndGet { case State(subs, counter, backlog) =>
                  State(subs, counter, backlog.enqueue(a))
                }
                .flatMap { case State(subs, _, _) =>
                  foreach(subs) { case ChannelState(channel, readySignal) => readySignal.get *> channel.send(a).void }
                }
                .as(PersistentTopic.rightUnit)
          }

        def subscribeAwait(maxQueued: Int): Resource[F, Stream[F, A]] =
          Resource
            .eval((Channel.bounded[F, A](maxQueued), F.deferred[Unit]).tupled)
            .flatMap { case (chan, readySignal) =>
              val subscribe = state
                .modify { case State(subs, id, backlog) =>
                  val channelState = ChannelState(chan, readySignal)
                  State(subs.updated(id, channelState), id + 1, backlog) -> (backlog, channelState, id)
                }
                .flatMap { case (backlog, ChannelState(chan, readySignal), id) =>
                  backlog.traverse_(chan.send) *> readySignal.complete(()) *>
                    id.pure[F]
                } <* subscriberCount
                .update(_ + 1)

              def unsubscribe(id: Long) =
                state.modify { case State(subs, nextId, backlog) =>
                  // _After_ we remove the bounded channel for this
                  // subscriber, we need to drain it to unblock to
                  // publish loop which might have already enqueued
                  // something.
                  def drainChannel: F[Unit] =
                    subs.get(id).traverse_ { chan =>
                      chan.channel.close >> chan.channel.stream.compile.drain
                    }

                  State(subs - id, nextId, backlog) -> drainChannel
                }.flatten >> subscriberCount.update(_ - 1)

              Resource
                .make(subscribe)(unsubscribe)
                .as(chan.stream)
            }

        def publish: Pipe[F, A, INothing] = { in =>
          (in ++ Stream.exec(close.void))
            .evalMap(publish1)
            .takeWhile(_.isRight)
            .drain
        }

        def subscribe(maxQueued: Int): Stream[F, A] =
          Stream.resource(subscribeAwait(maxQueued)).flatten

        def subscribers: Stream[F, Int] = subscriberCount.discrete

        def close: F[Either[PersistentTopic.Closed, Unit]] =
          signalClosure
            .complete(())
            .flatMap { completedNow =>
              val result = if (completedNow) PersistentTopic.rightUnit else PersistentTopic.closed

              state
                .get
                .flatMap { case State(subs, _, backlog) =>
                  foreach(subs) { case ChannelState(channel, deferred) =>
                    deferred.get *> channel.close.void
                  }
                }
                .as(result)
            }
            .uncancelable

        def closed: F[Unit] = signalClosure.get
        def isClosed: F[Boolean] = signalClosure.tryGet.map(_.isDefined)
      }
    }

  private final val closed: Either[Closed, Unit] = Left(Closed)
  private final val rightUnit: Either[Closed, Unit] = Right(())
}
