package duff.jagsql

import fs2._
import fs2.concurrent._

import cats.effect._
import cats.effect.implicits._

import cats.syntax.all._
import scala.collection.immutable.LongMap

abstract class PersistentTopic[F[_], A] { self =>

  /** Publishes elements from source of `A` to this topic. [[Pipe]] equivalent of `publish1`. Closes the topic when the
    * input stream terminates. Especially useful when the topic has a single producer.
    */
  def publish: Pipe[F, A, Nothing]

  /** Publishes one `A` to topic. No-op if the channel is closed, see [[close]] for further info.
    *
    * This operation does not complete until after the given element has been enqued on all subscribers, which means
    * that if any subscriber is at its `maxQueued` limit, `publish1` will semantically block until that subscriber
    * consumes an element.
    *
    * A semantically blocked publication can be interrupted, but there is no guarantee of atomicity, and it could result
    * in the `A` being received by some subscribers only.
    *
    * Note: if `publish1` is called concurrently by multiple producers, different subscribers may receive messages from
    * different producers in a different order.
    */
  def publish1(a: A): F[Either[PersistentTopic.Closed, Unit]]

  /** Subscribes for `A` values that are published to this topic.
    *
    * Pulling on the returned stream opens a "subscription", which allows up to `maxQueued` elements to be enqueued as a
    * result of publication.
    *
    * If at any point, the queue backing the subscription has `maxQueued` elements in it, any further publications
    * semantically block until elements are dequeued from the subscription queue.
    *
    * @param maxQueued
    *   maximum number of elements to enqueue to the subscription queue before blocking publishers
    */
  def subscribe(maxQueued: Int): Stream[F, A]

  /** Like `subscribe`, but represents the subscription explicitly as a `Resource` which returns after the subscriber is
    * subscribed, but before it has started pulling elements.
    */
  def subscribeAwait(maxQueued: Int): Resource[F, Stream[F, A]]

  /** Signal of current active subscribers.
    */
  def subscribers: Stream[F, Int]

  /** This method achieves graceful shutdown: when the topics gets closed, its subscribers will terminate naturally
    * after consuming all currently enqueued elements.
    *
    * "Termination" here means that subscribers no longer wait for new elements on the topic, and not that they will be
    * interrupted while performing another action: if you want to interrupt a subscriber, without first processing
    * enqueued elements, you should use `interruptWhen` on it instead.
    *
    * After a call to `close`, any further calls to `publish1` or `close` will be no-ops.
    *
    * Note that `close` does not automatically unblock producers which might be blocked on a bound, they will only
    * become unblocked if/when subscribers naturally finish to consume the respective elements. You can `race` the
    * publish with `close` to interrupt them immediately.
    */
  def close: F[Either[PersistentTopic.Closed, Unit]]

  /** Returns true if this topic is closed */
  def isClosed: F[Boolean]

  /** Semantically blocks until the topic gets closed. */
  def closed: F[Unit]

  /** Returns an alternate view of this `PersistentTopic` where its elements are of type `B`, given two functions, `A =>
    * B` and `B
    * => A`.
    */
  def imap[B](f: A => B)(g: B => A): PersistentTopic[F, B] =
    new PersistentTopic[F, B] {
      def publish: Pipe[F, B, Nothing] = sfb => self.publish(sfb.map(g))
      def publish1(b: B): F[Either[PersistentTopic.Closed, Unit]] = self.publish1(g(b))
      def subscribe(maxQueued: Int): Stream[F, B] =
        self.subscribe(maxQueued).map(f)
      def subscribeAwait(maxQueued: Int): Resource[F, Stream[F, B]] =
        self.subscribeAwait(maxQueued).map(_.map(f))
      def subscribers: Stream[F, Int] = self.subscribers
      def close: F[Either[PersistentTopic.Closed, Unit]] = self.close
      def isClosed: F[Boolean] = self.isClosed
      def closed: F[Unit] = self.closed
    }

}

object PersistentTopic {

  import scala.collection.immutable.Queue

  class AChannel

  type Closed = Closed.type
  object Closed

  case class PublishRange(from: Int, to: Int)
  case class PublishMessage(chan: AChannel, range: PublishRange)

  enum ChannelStatus {
    // case Reserved(range: PublishRange)
    case Free(at: Int)
  }

  /** Constructs a PersistentTopic */
  def apply[F[_], A](implicit F: Concurrent[F]): F[PersistentTopic[F, A]] =
    (
      F.ref((Map.empty[AChannel, ChannelStatus], 1L, Queue.empty[A])),
      SignallingRef[F, Int](0),
      F.deferred[Unit],
      FilteredQueue.empty[F, PublishMessage]
    ).mapN { case (state, subscriberCount, signalClosure, publishQueue) =>
      new PersistentTopic[F, A] {

        def foreach[B](lm: LongMap[B])(f: B => F[Unit]) =
          lm.foldLeft(F.unit) { case (op, (_, b)) => op >> f(b) }

        // split publish to have an update function that
        // does only publication
        // publication is called after all subscriptions and
        // all message publications, so we are sure all is sent.
        // Question is: are in order ?
        def publish1(a: A): F[Either[PersistentTopic.Closed, Unit]] =
          state
            .modify { case (subs, id, backlog) =>
              val newBacklog = backlog.enqueue(a)
              val backlogSize = newBacklog.size

              val toSend = subs.toList.map { case (channel, ChannelStatus.Free(at)) =>
                PublishMessage(channel, PublishRange(at, backlogSize))
              }

              val nSubs = subs.map { case (chan, status) => chan -> ChannelStatus.Free(backlogSize) }
              (
                (nSubs, id, newBacklog),
                (newBacklog, toSend)
              )
            }
            .flatMap { case (_, publishMessages) =>
              publishMessages.traverse_(publishQueue.offer)
            }
            .as(PersistentTopic.rightUnit)

        def subscribeAwait(maxQueued: Int): Resource[F, Stream[F, A]] =
          // Resource
          //   .eval(Channel.bounded[F, A](maxQueued))
          Resource
            .eval(F.pure(new AChannel))
            .evalMap { case chan =>
              // publish before setting in the state
              state.get.flatMap { case (_, _, backlog) =>
                (if (backlog.size != 0)
                   publishQueue.offer(PublishMessage(chan, PublishRange(0, backlog.size)))
                 else F.unit).as(chan -> backlog.size)
              }
            }
            .flatMap { (chan, lastIndex) =>
              val subscribe: F[AChannel] = state
                .update { case (subs, id, backlog) =>
                  val nsubs = subs.updated(chan, ChannelStatus.Free(lastIndex))
                  ((nsubs, id + 1, backlog))
                }
                .as(chan) <* subscriberCount.update(_ + 1)

              def unsubscribe(chan: AChannel) =
                state.modify { case (subs, nextId, backlog) =>
                  // you should make sure there is no channel request in the queue
                  // or you will block the others
                  // def drainChannel: F[Unit] =
                  // here do something clever to purge the
                  // queue
                  //  publishQueue.take
                  // subs.get(chan).traverse_ { case (chan, isOpen) =>
                  //   chan.close >> chan.stream.compile.drain
                  // }

                  (subs - chan, nextId, backlog) -> F.unit
                }.flatten >> subscriberCount.update(_ - 1)

              def s(a: AChannel): Stream[F, A] = publishQueue
                .toStream { case PublishMessage(chan, message) =>
                  chan == a
                }
                .evalMap { case PublishMessage(chan, PublishRange(from, to)) =>
                  state.get.map { case (_, _, backlog) =>
                    val d = backlog.slice(from, to)
                    Stream.emits(d)
                  }
                }
                .flatten

              Resource
                .make(subscribe)(unsubscribe)
                .map(s)
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
            .map { completedNow =>
              val result = if (completedNow) PersistentTopic.rightUnit else PersistentTopic.closed
              result

            // state
            //   .get
            //   .flatMap { case (subs, _, backlog) => foreach(subs)(_._1.close.void) }
            //   .as(result)
            // ???
            }
            .uncancelable

        def closed: F[Unit] = signalClosure.get
        def isClosed: F[Boolean] = signalClosure.tryGet.map(_.isDefined)
      }
    }

  private final val closed: Either[Closed, Unit] = Left(Closed)
  private final val rightUnit: Either[Closed, Unit] = Right(())
}
