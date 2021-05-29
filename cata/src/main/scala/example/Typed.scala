package example

import cats._
import cats.implicits._

object Typed extends App {

  trait CovariantFunctor[F[_]] {
    def fmap[A, B](a: F[A], f: A => B): F[B]
  }
  trait Contra[F[_]] {
    def cmap[A, B](a: F[A], f: B => A): F[B]
  }

  import cats.data.NonEmptyList
  sealed trait Ticket[F[_]] {
    val movie: String
    val seats: F[String]
  }
  final case class SingleTicket(
      movie: String,
      seats: String
  ) extends Ticket[Id]
  final case class GroupTicket(
      movie: String,
      seats: List[String]
  ) extends Ticket[List]

  // both
  final case class Cofree[F[_], A](head: A, tail: F[Cofree[F, A]])
  // either
  final case class Free[F[_], A](resume: Either[A, F[Free[F, A]]])

  object Free {
    def pure[F[_], A](a: A) = Free(Left(a))
    def suspend[F[_], A](f: F[Free[F, A]]): Free[F, A] = Free(Right(f))
  }
  final case class Fix[F[_]](unfix: F[Fix[F]])
  // conceptually
  // IO = Free[Par, ]
  // Stream = Cofree[]

  trait Process[I] {
    def next: Option[(I, Process[I])]
  }

  sealed trait ExpressionF[A]
  case class AddF[A](left: A, right: A) extends ExpressionF[A]
  case class LiteralF[A](value: Int) extends ExpressionF[A]

  type ExpressionFixed = Fix[ExpressionF] // === Expression[Fix[Expression]]

  // declaring the type is important for some reason
  val didier: Fix[ExpressionF] = Fix(AddF(Fix(LiteralF(1)), Fix(LiteralF(1))))

  implicit val expressionFIsFunctor: Functor[ExpressionF] =
    new Functor[ExpressionF] {
      def map[A, B](fa: ExpressionF[A])(f: A => B): ExpressionF[B] = fa match {
        case AddF(l, r)  => AddF(f(l), f(r))
        case LiteralF(v) => LiteralF(v)
      }
    }

  // generic catamorphism
  def cata[F[_]: Functor, K](alg: F[K] => K)(e: Fix[F]): K = {
    alg(e.unfix.map(cata(alg)))
  }

  // can be easily extented for cofree, no idea if that is useful
  def catacofree[F[_]: Functor, K, A](alg: F[K] => K)(e: Cofree[F, A]): K = {
    alg(e.tail.map(catacofree(alg)))
  }

  def evalExprF(e: ExpressionF[Int]): Int = e match {
    case LiteralF(v)  => v
    case AddF(e1, e2) => e1 + e2
  }

  def allLiterals[K](
      e: ExpressionF[List[ExpressionFixed]]
  ): List[ExpressionFixed] = e match {
    case AddF(left, right)   => left ++ right
    case e @ LiteralF(value) => List(Fix(LiteralF(value)))
  }

  def allLiterals2[K](
      e: ExpressionF[List[ExpressionF[K]]]
  ): List[ExpressionF[K]] = e match {
    case AddF(left, right)   => left ++ right
    case e @ LiteralF(value) => List(LiteralF(value))
  }

  def toStringEf(e: ExpressionF[String]): String = e match {
    case AddF(left, right) => s"AddF($left, $right)"
    case LiteralF(value)   => value.toString
  }

  val f = cata(evalExprF) _

  val fp = catacofree(evalExprF) _

  val g = cata(allLiterals2) _

  println(f(didier))
  println(cata(toStringEf)(didier))
  println(g(didier))
}
