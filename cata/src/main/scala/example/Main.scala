package example

object Main extends App {
  
  //def main(args: Array[String]): Unit = ()

  trait Functor[F[_]] {
    def map[A, B](a: F[A], f: A => B): F[B]
  }

  object Functor {
    def apply[F[_]: Functor]: Functor[F] = implicitly
  }
  implicit class FunctorSyntax[F[_]: Functor, A](a: F[A]) {
    def map[B](f: A => B): F[B] = Functor[F].map(a, f)
  }

  case class Fix[F[_]](unfix: F[Fix[F]])

  // sealed trait Expression
  // case class Add(left: Expression, right: Expression) extends Expression
  // case class Literal(value: Int) extends Expression

  sealed trait ExpressionF[A]
  case class AddF[A](left: A, right: A) extends ExpressionF[A]
  case class LiteralF[A](value: Int) extends ExpressionF[A]

  type ExpressionFixed = Fix[ExpressionF] // === Expression[Fix[Expression]]

// declaring the type is important for some reason
  val didier: Fix[ExpressionF] = Fix(AddF(Fix(LiteralF(1)), Fix(LiteralF(1))))

  // def evalF(e: ExpressionFixed): Int = e.unfix match {
  //   case AddF(left, right) => evalF(left) + evalF(right)
  //   case LiteralF(v)       => v
  // }

  // dead stupid functor "structural"
  implicit val expressionFIsFunctor: Functor[ExpressionF] =
    new Functor[ExpressionF] {
      def map[A, B](a: ExpressionF[A], f: A => B): ExpressionF[B] = a match {
        case AddF(l, r)  => AddF(f(l), f(r))
        case LiteralF(v) => LiteralF(v)
      }
    }
  
  // def almostCata(alg: (ExpressionF[Int] => Int))(e: Fix[ExpressionF]): Int = {
  //   alg(e.unfix.map(almostCata(alg)))
  // }

  // generic catamorphism
  def cata[F[_] : Functor, K](alg: F[K] => K)(e: Fix[F]): K = {
    alg(e.unfix.map(cata(alg)))
  }

  // reverse arrows, whatever that means
  def ana[F[_] : Functor, K](coalg: K => F[K])(e: K): Fix[F] = {
    val k = coalg(e) map ana(coalg) _
    Fix(k)
  }


  def evalExprF(e: ExpressionF[Int]): Int = e match {
    case LiteralF(v) => v
    case AddF(e1, e2) => e1 + e2
  }

  def toStringEf(e: ExpressionF[String]): String = e match {
    case AddF(left, right) => s"AddF($left, $right)"
    case LiteralF(value) => value.toString
  }

  val f = cata(evalExprF) _

  println(f(didier))
  println(cata(toStringEf)(didier))
  // println(evalF(didier))
}
