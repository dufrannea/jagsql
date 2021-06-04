package duff
package ast

import cats._
import cats.implicits._

import scala.language.experimental
import scala.util.Try
import duff.cst.Operator

enum SimpleType {
  case Number
  case String
  case Bool

}

enum Type {
  case Simple(st: SimpleType)
  case Any
}

object Type {
  val Number = Type.Simple(SimpleType.Number)
  val String = Type.Simple(SimpleType.String)
  val Bool = Type.Simple(SimpleType.Bool)
}

import Type._

enum Function(val args: List[Type], val returnType: Type) {
  case file extends Function(Type.String :: Nil, Type.String)
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
      case cst.Literal.RegexLiteral(_)  => Type.String
      case cst.Literal.StringLiteral(_) => Type.String
      case cst.Literal.NumberLiteral(_) => Type.Number
      case cst.Literal.BoolLiteral(_)   => Type.Bool
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
      leftA      <- analyzeExpression(left)
      rightA     <- analyzeExpression(right)
      leftType = leftA.unfix.expressionType
      rightType = rightA.unfix.expressionType
      commonType <- if (leftType == rightType) Either.right(leftA.unfix.expressionType)
                    else
                      s"Types on the LHS and RHS should be the same for operator ${operator.toString}, here found ${leftType.toString} and ${rightType.toString}".asLeft
      resultType <- (commonType, operator) match {
                      // Equality is valid for every type
                      case (_, Operator.Equal) | (_, Operator.Different)           =>
                        Either.right(Type.Bool)
                      // Int specific comparisons
                      case (Type.Number, o)
                          if o == Operator.Less || o == Operator.More || o == Operator.LessEqual || o == Operator.MoreEqual =>
                        Either.right(Type.Bool)
                      // Int specific operations
                      case (Type.Number, o)
                          if o == Operator.Plus || o == Operator.Minus || o == Operator.Divided || o == Operator.Times =>
                        Either.right(Type.Number)
                      // Only string operator is concatenation
                      case (Type.String, Operator.Plus)                            => Either.right(Type.String)
                      case (Type.Bool, o) if o == Operator.And || o == Operator.Or => Either.right(Type.Bool)
                      case _ => s"Operator ${operator.toString} cannot be used with type ${leftType.toString}".asLeft
                    }
    } yield Fix(ExpressionF.Binary(leftA, rightA, operator, resultType))

  case cst.Expression.Unary(e) => sys.error("not yet implemented")
}
