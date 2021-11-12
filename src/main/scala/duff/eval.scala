package duff.jagsql
package eval

import duff.jagsql.ast.*
import duff.jagsql.cst.Literal
import duff.jagsql.std.*

import cats.*
import cats.data.{Kleisli, Reader}
import cats.implicits.*

enum Value {
  case VString(e: String)
  case VNumber(e: BigDecimal)
  case VBoolean(e: Boolean)
  case VArray(values: List[Value])
  case Error
}

import duff.jagsql.cst.Operator
import duff.jagsql.eval.Value.*

type ExpressionValue = Reader[Map[String, Value], Value]

def alg(e: ExpressionF[ExpressionValue]): ExpressionValue = e match {
  case ExpressionF.LiteralExpression(l, _)                     =>
    val value = l match {
      case Literal.BoolLiteral(e)   => VBoolean(e)
      case Literal.StringLiteral(e) => VString(e)
      case Literal.NumberLiteral(e) => VNumber(e)
      case Literal.RegexLiteral(e)  => Error
    }
    Kleisli(_ => value)
  case ExpressionF.Binary(left, right, operator, commonType)   =>
    val v = ((left, right)).tupled.map { case (l, r) => (l, r, operator) }.map {
      case (left, right, Operator.Equal)                => VBoolean(left == right)
      case (left, right, Operator.Different)            => VBoolean(left != right)
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
  case ExpressionF.FunctionCallExpression(array, arguments, _) =>
    Kleisli { values =>
      val k = arguments.map { argument =>
        argument.run(values)
      }
      VArray(k.toList)
    }
  case ExpressionF.FieldRef(tableId, fieldId, _)               =>
    Kleisli { values =>
      values.get(fieldId).getOrElse(sys.error(s"Key not found $fieldId in ${values.toString}"))
    }
  case _                                                       => ???
}

def eval(e: ast.Expression): ExpressionValue =
  cata(alg)(e)
