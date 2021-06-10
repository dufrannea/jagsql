package duff
package eval

import ast._
import utils._

import duff.cst.Literal
import cats._
import cats.implicits._

enum Value {
  case VString(e: String)
  case VNumber(e: BigDecimal)
  case VBoolean(e: Boolean)
  case Error
}

import Value._
import cst.Operator

def alg(e: ExpressionF[Value]): Value = e match {
  case ExpressionF.LiteralExpression(l, _)                   =>
    l match {
      case Literal.BoolLiteral(e)   => VBoolean(e)
      case Literal.StringLiteral(e) => VString(e)
      case Literal.NumberLiteral(e) => VNumber(e)
      case Literal.RegexLiteral(e)  => Error
    }
  case ExpressionF.Binary(left, right, operator, commonType) =>
    (left, right, operator) match {
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

  case _ => Error
}

def eval(e: ast.Expression): Value = {
  cata(alg)(e)
}
