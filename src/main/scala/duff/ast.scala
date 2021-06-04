package duff
package ast

import cats._
import cats.implicits._

import scala.language.experimental
import scala.util.Try

enum SimpleType {
  case Number
  case String
}

enum Type {
  case Simple(st: SimpleType)
  case Any
}

import Type._
import SimpleType._

enum Function(val args: List[Type], val returnType: Type) {
  case file extends Function(Simple(String) :: Nil, Simple(String))
}

enum ExpressionF[K] {

  def expressionType: Type

  case LiteralExpression(literal: cst.Literal, expressionType: Type)
  case FunctionCallExpression(name: String, arguments: Seq[K], expressionType: Type)
  case Binary(left: K, right: K, operator: cst.Operator, expressionType: Type)
  case Unary(expression: K, expressionType: Type)
}

object ExpressionF {

  given Functor[ExpressionF] with

    def map[A, B](fa: ExpressionF[A])(f: A => B): ExpressionF[B] = fa match {
      case FunctionCallExpression(name, args, t) => FunctionCallExpression(name, args.map(f), t)
      case Binary(left, right, operator, t)      => Binary(f(left), f(right), operator, t)
      case Unary(e, t)                           => Unary(f(e), t)
      case LiteralExpression(literal, t)         => LiteralExpression(literal, t)
    }

}

case class Fix[F[_]](val unfix: F[Fix[F]])

type Expression = Fix[ExpressionF]

def analyzeExpression(e: cst.Expression): Either[String, Fix[ExpressionF]] = e match {
  case cst.Expression.LiteralExpression(l)                    =>
    val expressionType = l match {
      // TODO: actually depending on the regex literal we can either
      // return an array or a string, that is the goal of it :)
      case cst.Literal.RegexLiteral(_)  => Type.Simple(SimpleType.String)
      case cst.Literal.StringLiteral(_) => Type.Simple(SimpleType.String)
      case cst.Literal.NumberLiteral(_) => Type.Simple(SimpleType.Number)
    }

    Right(Fix(ExpressionF.LiteralExpression(l, expressionType)))
  case cst.Expression.FunctionCallExpression(name, arguments) =>
    val function = Function.valueOf(name)

    for {
      analyzedArguments <- arguments.traverse(analyzeExpression(_))
      zippedArgsWithExpectedTypes = function.args.zip(analyzedArguments)
      _ <- zippedArgsWithExpectedTypes.foldM(()) { case (_, (expectedArgType, Fix(analyzedExpression))) =>
             analyzedExpression.expressionType match {
               case `expectedArgType` => Either.unit
               case _                 =>
                 Left(
                   s"Error in function $name, argument with type ${analyzedExpression.expressionType} expected to have type $expectedArgType"
                 )
             }
           }
    } yield Fix(ExpressionF.FunctionCallExpression(name, analyzedArguments, function.returnType))

  case cst.Expression.Binary(left, right, operator) =>
    for {
      leftA  <- analyzeExpression(left)
      rightA <- analyzeExpression(right)
      _      <- if (leftA.unfix.expressionType == rightA.unfix.expressionType) Either.unit else "wrongtypes".asLeft
    } yield Fix(ExpressionF.Binary(leftA, rightA, operator, leftA.unfix.expressionType))
  // ExpressionF.LiteralExpression(left, right, operator)

  case cst.Expression.Unary(e) => sys.error("not yet implemented")
}