package duff.jagsql
package ast

import duff.jagsql.ast.ExpressionF.{Binary, FieldRef, FunctionCallExpression, LiteralExpression, Unary}
import duff.jagsql.cst.{Operator, Position}
import duff.jagsql.eval.Value
import duff.jagsql.std.*
import duff.jagsql.std.RegexWrapper

import java.util

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap
import scala.language.experimental
import scala.util.Try

import cats.*
import cats.data.*
import cats.implicits.*

object Function {

  def valueOf(s: String): Function =
    s match {
      case "file"         => file
      case "array"        => array
      case "max"          => max
      case "to_int"       => to_int
      case "run"          => run
      case "regex"        => regex
      case "regex_struct" => regex_struct
      case "struct"       => struct
      case _              => throw new IllegalArgumentException(s"No Function named $s")
    }

}

// TODO: need generic types here, or completely erased
sealed trait Function {
  def validateArgs(candidateArgs: List[ast.Expression]): Either[(Int, String), Type]

  protected def formatError(index: Int, leftType: String, rightType: String) =
    s"Error in function, argument at index $index with type $leftType expected to have type $rightType"
}

sealed trait Monomorphic extends Function {

  override def validateArgs(candidateArgs: List[ast.Expression]): Either[(Int, String), Type] = {
    val argsList = args.to(LazyList) ++ maybeVariadic.map(LazyList.continually _).getOrElse(Nil)
    val err = argsList
      .zip(candidateArgs.map(_.unfix.expressionType))
      .zipWithIndex
      .find { case ((left, right), index) => !left.isCompatibleWith(right) }
    err match {
      case Some((leftType, rightType), index) =>
        val errorMessage = formatError(index, leftType.toString, rightType.toString)
        Left((index -> errorMessage))
      case None                               => Right(returnType)
    }
  }

  val args: List[Type]
  val returnType: Type
  val maybeVariadic: Option[Type] = None
}

case object file extends Monomorphic {
  val args = Type.String :: Nil
  val returnType = Type.String
}

case object array extends Monomorphic {
  val args = Nil
  val returnType = Type.Number
  override val maybeVariadic: Option[Type] = Some(Type.Number)
}

case object to_int extends Monomorphic {
  val args = Type.String :: Nil
  val returnType = Type.Number
  override val maybeVariadic: Option[Type] = None
}

case object run extends Monomorphic {
  val args = Type.String :: Nil
  val returnType = Type.String
  override val maybeVariadic: Option[Type] = None
}

case object regex extends Function {

  override def validateArgs(candidateArgs: List[ast.Expression]): Either[(Int, String), Type] =
    candidateArgs.map(_.unfix.expressionType) match {
      case (_: SimpleType.Regex) :: Type.String :: Nil                  => Right(SimpleType.Array(Type.String))
      case x :: Type.String :: Nil if !x.isInstanceOf[SimpleType.Regex] =>
        Left((0, formatError(0, x.toString, "Regex")))
      case (_: SimpleType.Regex) :: x :: Nil if x != Type.String        =>
        Left((1, formatError(1, x.toString, "String")))
      case _                                                            => Left((2, "Too many arguments"))
    }

}

case object regex_struct extends Function {

  override def validateArgs(candidateArgs: List[ast.Expression]): Either[(Int, String), Type] =
    candidateArgs.map(_.unfix.expressionType) match {
      case (r: SimpleType.Regex) :: Type.String :: Nil =>
        val groups = r
          .w
          .groups
          .map { case (index, maybeName) =>
            maybeName.getOrElse(s"group_$index") -> Type.String
          }
          .toList
        NonEmptyList.fromList(groups) match {
          case Some(g) => Right(SimpleType.Struct(g.toNem))
          case None    => Left((0, "At least one group should be defined"))
        }

      case x :: Type.String :: Nil if !x.isInstanceOf[SimpleType.Regex] =>
        Left((0, formatError(0, x.toString, "Regex")))
      case (_: SimpleType.Regex) :: x :: Nil if x != Type.String        =>
        Left((1, formatError(1, x.toString, "String")))
      case _                                                            => Left((2, "Too many arguments"))
    }

}

case object struct extends Function {

  override def validateArgs(candidateArgs: List[ast.Expression]): Either[(Int, String), Type] =
    def validate(z: List[ast.ExpressionF[_]]): Either[String, List[(String, SimpleType)]] =
      z match {
        case ExpressionF.LiteralExpression(cst.Literal.StringLiteral(s), _) :: e :: t
            if e.expressionType.isInstanceOf[SimpleType] =>
          validate(t).map { tail =>
            (s -> e.expressionType.asInstanceOf[SimpleType]) :: tail
          }
        case Nil => Right(Nil)
        case _   => Left("woooz")
      }

    validate(candidateArgs.map(_.unfix)) match {
      case Left(e)              => Left((0, e))
      case Right(validatedArgs) =>
        val argsMap = validatedArgs
        NonEmptyList.fromList(argsMap).map(_.toNem) match {
          case Some(nem) =>
            val structType = SimpleType.Struct(nem)
            Right(structType)
          case None      => Left((0, "Call to function struct should at least have one argument"))
        }
    }

}

// TODO: AggregationFunction takes only one argument ?
sealed trait AggregateFunction(
  val args: List[Type],
  val returnType: Type,
  override val maybeVariadic: Option[Type] = None
) extends Monomorphic {

  def run(values: List[eval.Value]): Value
}

case object max extends AggregateFunction(Type.Number :: Nil, Type.Number) {

  def run(values: List[eval.Value]): Value =
    values.maxBy {
      case eval.Value.VNumber(n) => n
      case value                 => sys.error(s"Value is not a number $value, function 'max' only work for numbers")
    }

}

enum ExpressionF[K] {

  def expressionType: Type

  case LiteralExpression(literal: cst.Literal, expressionType: Type)
  case FunctionCallExpression(function: Function, arguments: Seq[K], expressionType: Type)
  case Binary(left: K, right: K, operator: cst.Operator, expressionType: Type)
  case Unary(expression: K, operator: cst.Operator, expressionType: Type)
  case FieldRef(tableId: String, fieldId: String, expressionType: Type)
  case StructAccess(expression: K, members: NonEmptyList[String], expressionType: Type)
}

object ExpressionF {

  given Traverse[ExpressionF] with

    def foldLeft[a, b](fa: ExpressionF[a], b: b)(f: (b, a) => b): b =
      fa match {
        case FunctionCallExpression(name, args, t) => args.foldLeft(b)(f)
        case Binary(left, right, _, t)             => f(f(b, left), right)
        case Unary(e, _, t)                        => f(b, e)
        case LiteralExpression(literal, t)         => b
        case FieldRef(tableId, fieldId, t)         => b
        case StructAccess(inner, _, t)             => f(b, inner)
      }

    def foldRight[a, b](fa: ExpressionF[a], lb: cats.Eval[b])(f: (a, cats.Eval[b]) => cats.Eval[b]): cats.Eval[b] =
      fa match {
        case FunctionCallExpression(name, args, t) => args.foldRight(lb)(f)
        case Binary(left, right, _, t)             => f(left, Eval.defer(f(right, lb)))
        case Unary(e, _, t)                        => f(e, lb)
        case LiteralExpression(literal, t)         => lb
        case FieldRef(tableId, fieldId, t)         => lb
        case StructAccess(inner, _, t)             => f(inner, lb)
      }

    def traverse[G[_]: Applicative, A, B](fa: ExpressionF[A])(f: A => G[B]): G[ExpressionF[B]] =
      fa match {
        case FunctionCallExpression(name, args, t) => args.traverse(f).map(a => FunctionCallExpression(name, a, t))
        case Binary(left, right, operator, t)      =>
          (f(left), f(right)).mapN { case (l, r) => Binary(l, r, operator, t) }
        case Unary(e, operator, t)                 => f(e).map(Unary(_, operator, t))
        case LiteralExpression(literal, t)         => LiteralExpression(literal, t).pure[G]
        case FieldRef(tableId, fieldId, t)         => FieldRef(tableId, fieldId, t).pure[G]
        case StructAccess(inner, accessors, t)     => f(inner).map(StructAccess(_, accessors, t))
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
  case r @ FunctionCallExpression(name, args, t) => p(r) || args.contains(true)
  case r @ Binary(left, right, operator, t)      => p(r) || left || right
  case r                                         => p(r)
}

type Expression = Fix[ExpressionF]

case class Labeled[F[_]: Functor](unlabel: F[Labeled[F]], a: Position) {
  def fix: Fix[F] = Fix(unlabel.map(_.fix))
}

type IndexedExpression = Labeled[ExpressionF]
