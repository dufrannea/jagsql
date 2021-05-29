package example

import cats._
import cats.implicits._
import cats.free.Free
import cats.free.Free._
import cats.data.Op

object CliOperations {
  type Fqdn = String
  type Handle = String
  type Url = String

  sealed trait CliOperationK[K]
  case class CliPause(dataset: Fqdn) extends CliOperationK[Unit]
  case class CliResume(dataset: Fqdn) extends CliOperationK[Unit]
  case class CliBackfill(dataset: Fqdn) extends CliOperationK[Handle]
  case class CliDryrun(dataset: Fqdn, partition: String)
      extends CliOperationK[Url]

  type Operation[A] = Free[CliOperationK, A]

  def pause(dataset: Fqdn): Operation[Unit] =
    liftF[CliOperationK, Unit](CliPause(dataset))
  def resume(dataset: Fqdn): Operation[Unit] =
    liftF[CliOperationK, Unit](CliResume(dataset))
  def backfill(dataset: Fqdn): Operation[Handle] =
    liftF[CliOperationK, Handle](CliBackfill(dataset))
  // you get the idea
  def dryrun(dataset: Fqdn, partition: String): Operation[Url] =
    liftF[CliOperationK, Handle](CliDryrun(dataset, partition))

  def backfillDataset(s: Fqdn): Operation[Handle] = {
    for {
      _ <- pause(s)
      backfillHandle <- backfill(s)
    } yield backfillHandle
  }

  object Compiler {
    import cats.data.WriterT
    import cats.data.WriterT._

    import cats.arrow.FunctionK
    import cats.{Id, ~>}
    import scala.collection.mutable
    import cats.effect._
    import cats.data.{Writer, Chain}

    def compile: CliOperationK ~> Id = new (CliOperationK ~> Id) {
      def apply[A](fa: CliOperationK[A]): Id[A] = {
        fa match {
          case CliPause(dataset) =>
            println(s"pausing $dataset")
          case CliResume(dataset) =>
            println(s"resuming $dataset")
          case CliBackfill(dataset) =>
            println(s"backfilling $dataset")
            "42"
          case CliDryrun(dataset, partition) =>
            println(s"Dryrun of $dataset.$partition")
            "http://somewhere"
        }
      }
    }

    type WLS[A] = Writer[Chain[String], A]

    def compileWriter: CliOperationK ~> WLS = new (CliOperationK ~> WLS) {
      def apply[A](fa: CliOperationK[A]): WLS[A] = {
        fa match {
          case CliPause(dataset) =>
            Writer.tell(Chain(s"pause $dataset"))
          case CliResume(dataset) =>
            Writer.tell(Chain(s"resuming $dataset"))
          case CliBackfill(dataset) =>
            Writer.tell(Chain(s"backfilling $dataset")) *> Writer.value("42")
          case CliDryrun(dataset, partition) =>
            Writer.tell(Chain(s"Dryrun of $dataset.$partition")) *> Writer.value(
              "http://somewhere"
            )
        }
      }
    }

    def compileIO: CliOperationK ~> IO = new (CliOperationK ~> IO) {
      def apply[A](fa: CliOperationK[A]): IO[A] = {
        fa match {
          case CliPause(dataset) =>
            IO(println(s"pausing $dataset"))
          case CliResume(dataset) =>
            IO(println(s"resuming $dataset"))
          case CliBackfill(dataset) =>
            println(s"backfilling $dataset")
            IO("42")
          case CliDryrun(dataset, partition) =>
            println(s"Dryrun of $dataset.$partition")
            IO("http://somewhere")
        }
      }
    }

    type IOWriter[A] = WriterT[IO, String, A]

    def toWriter[A](a: IO[A]): IOWriter[A] =
        WriterT.tell[IO, String]("lol") *> WriterT.liftF(a)
    
  }

  def main(args: Array[String]): Unit = {
    val r = backfillDataset("didier").foldMap(Compiler.compileWriter)
    val s = r.run
    println(s)
  }

}
