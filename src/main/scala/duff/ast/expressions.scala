package duff.jagsql
package ast

import duff.jagsql.ast.ExpressionF.{Binary, FieldRef, FunctionCallExpression, LiteralExpression, Unary}
import duff.jagsql.cst.{Indexed, Operator, Position}
import duff.jagsql.std.*

import scala.language.experimental
import scala.util.Try

import cats.*
import cats.data.*
import cats.implicits.*

object Function {

  def valueOf(s: String): Function =
    s match {
      case "file"  => file
      case "array" => array
      case "max"   => max
      case _       => throw new IllegalArgumentException(s"No Function named $s")
    }

}

// TODO: need generic types here, or completely erased
sealed trait Function {
  val args: List[Type]
  val returnType: Type
  val maybeVariadic: Option[Type] = None
}

case object file extends Function {
  val args = Type.String :: Nil
  val returnType = Type.String
}

case object array extends Function {
  val args = Nil
  val returnType = Type.Number
  override val maybeVariadic: Option[Type] = Some(Type.Number)
}

// TODO: AggregationFunction takes only one argument ?
sealed trait AggregateFunction(
  val args: List[Type],
  val returnType: Type,
  override val maybeVariadic: Option[Type] = None
) extends Function

case object max extends AggregateFunction(Type.Number :: Nil, Type.Number)

enum ExpressionF[K] {

  def expressionType: Type

  case LiteralExpression(literal: cst.Literal, expressionType: Type)
  case FunctionCallExpression(function: Function, arguments: Seq[K], expressionType: Type)
  case Binary(left: K, right: K, operator: cst.Operator, expressionType: Type)
  case Unary(expression: K, expressionType: Type)
  case FieldRef(tableId: String, fieldId: String, expressionType: Type)
}

object ExpressionF {

  given Traverse[ExpressionF] with

    def foldLeft[a, b](fa: ExpressionF[a], b: b)(f: (b, a) => b): b =
      fa match {
        case FunctionCallExpression(name, args, t) => args.foldLeft(b)(f)
        case Binary(left, right, operator, t)      => f(f(b, left), right)
        case Unary(e, t)                           => f(b, e)
        case LiteralExpression(literal, t)         => b
        case FieldRef(tableId, fieldId, t)         => b
      }

    def foldRight[a, b](fa: ExpressionF[a], lb: cats.Eval[b])(f: (a, cats.Eval[b]) => cats.Eval[b]): cats.Eval[b] =
      fa match {
        case FunctionCallExpression(name, args, t) => args.foldRight(lb)(f)
        case Binary(left, right, operator, t)      => f(right, f(left, lb))
        case Unary(e, t)                           => f(e, lb)
        case LiteralExpression(literal, t)         => lb
        case FieldRef(tableId, fieldId, t)         => lb
      }

    def traverse[G[_]: Applicative, A, B](fa: ExpressionF[A])(f: A => G[B]): G[ExpressionF[B]] =
      fa match {
        case FunctionCallExpression(name, args, t) => args.traverse(f).map(a => FunctionCallExpression(name, a, t))
        case Binary(left, right, operator, t)      =>
          (f(left), f(right)).mapN { case (l, r) => Binary(l, r, operator, t) }
        case Unary(e, t)                           => f(e).map(Unary(_, t))
        case LiteralExpression(literal, t)         => LiteralExpression(literal, t).pure[G]
        case FieldRef(tableId, fieldId, t)         => FieldRef(tableId, fieldId, t).pure[G]
      }

  extension (e: Expression) {
    def exists(p: ExpressionF[_] => Boolean) = cata(existsAlg(p))(e)
    def forAll(p: ExpressionF[_] => Boolean) = cata(allAlg(p))(e)
  }

}

def allAlg(p: ExpressionF[_] => Boolean)(a: ExpressionF[Boolean]): Boolean = a match {
  case r @ FunctionCallExpression(name, args, t) => p(r) && args.forall(_ == true)
  case r @ Binary(left, right, operator, t)      => p(r) && left && right
  case r                                         => p(r)
}

def existsAlg(p: ExpressionF[_] => Boolean)(a: ExpressionF[Boolean]): Boolean = a match {
  case r @ FunctionCallExpression(name, args, t) => p(r) || args.exists(_ == true)
  case r @ Binary(left, right, operator, t)      => p(r) || left || right
  case r                                         => p(r)
}

type Expression = Fix[ExpressionF]

case class Labeled[F[_]: Functor](unlabel: F[Labeled[F]], a: Position) {
  def fix: Fix[F] = Fix(unlabel.map(_.fix))
}

type IndexedExpression = Labeled[ExpressionF]
