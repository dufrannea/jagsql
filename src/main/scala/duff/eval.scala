package duff
package eval

import ast._
import utils._

import duff.cst.Literal
import cats._
import cats.implicits._
import cats.data.Reader
import cats.data.Kleisli

enum Value {
  case VString(e: String)
  case VNumber(e: BigDecimal)
  case VBoolean(e: Boolean)
  case Error
}

import Value._
import cst.Operator

type ExpressionValue = Reader[Map[String, Value], Value]

def alg(e: ExpressionF[ExpressionValue]): ExpressionValue = e match {
  case ExpressionF.LiteralExpression(l, _)                   =>
    val value = l match {
      case Literal.BoolLiteral(e)   => VBoolean(e)
      case Literal.StringLiteral(e) => VString(e)
      case Literal.NumberLiteral(e) => VNumber(e)
      case Literal.RegexLiteral(e)  => Error
    }
    Kleisli(_ => value)
  case ExpressionF.Binary(left, right, operator, commonType) =>
    val v = ((left, right)).tupled.map { case (l, r) => (l, r, operator) }.map {
      case (_, _, Operator.Equal)                       => VBoolean(left == right)
      case (_, _, Operator.Different)                   => VBoolean(left != right)
      case (VNumber(i), VNumber(j), Operator.Less)      => VBoolean(i < j)
      case (VNumber(i), VNumber(j), Operator.More)      => VBoolean(i > j)
      case (VNumber(i), VNumber(j), Operator.LessEqual) => VBoolean(i <= j)
      case (VNumber(i), VNumber(j), Operator.MoreEqual) => VBoolean(j <= i)
      case (VNumber(i), VNumber(j), Operator.Plus)      => VNumber(i + j)
      case (VNumber(i), VNumber(j), Operator.Minus)     => VNumber(i - j)
      case (VNumber(i), VNumber(j), Operator.Times)     => VNumber(i * j)
      case (VNumber(i), VNumber(j), Operator.Divided)   => VNumber(i / j)
      // Only string operator is concatenation
      case (VString(i), VString(j), Operator.Plus)      => VString(i + j)
      case (VBoolean(i), VBoolean(j), Operator.And)     => VBoolean(i && j)
      case (VBoolean(i), VBoolean(j), Operator.Or)      => VBoolean(i || j)
      case _                                            => Error
    }
    v
  case ExpressionF.FieldRef(value, _)                        =>
    Kleisli { values =>
      values.get(value).getOrElse(sys.error(s"Key not found $value in ${values.toString}"))
    }
  case _                                                     => ???
}

def eval(e: ast.Expression): ExpressionValue = {
  cata(alg)(e)
}
