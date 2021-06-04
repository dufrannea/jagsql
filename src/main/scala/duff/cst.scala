package duff
package cst

import cats.data
import cats.data.NonEmptyList
import Literal.NumberLiteral
import Literal.RegexLiteral
import Literal.StringLiteral

import java.nio.file.Path
import scala.math.BigDecimal

enum Literal {
  case StringLiteral(a: String)
  case NumberLiteral(a: BigDecimal)
  case RegexLiteral(str: String)
  case BoolLiteral(value: Boolean)
}

object Literal {
  def string(s: String): StringLiteral = StringLiteral(s)
  def int(s: BigDecimal): NumberLiteral = NumberLiteral(s)
  def regex(s: String): RegexLiteral = RegexLiteral(s)
}

enum BinaryOperator {
  case Equal, Different, Less, More, LessEqual, MoreEqual, Plus, Minus, Times, Divided, Or, And
}

enum UnaryOperator {
  case Not
}

enum Operator {
  case B(op: BinaryOperator)
  case U(op: UnaryOperator)
}

object Operator {
  val Equal = B(BinaryOperator.Equal)
  val Different = B(BinaryOperator.Different)
  val Less = B(BinaryOperator.Less)
  val More = B(BinaryOperator.More)
  val LessEqual = B(BinaryOperator.LessEqual)
  val MoreEqual = B(BinaryOperator.MoreEqual)
  val Plus = B(BinaryOperator.Plus)
  val Minus = B(BinaryOperator.Minus)
  val Times = B(BinaryOperator.Times)
  val Divided = B(BinaryOperator.Divided)
  val Or = B(BinaryOperator.Or)
  val And = B(BinaryOperator.And)
}

enum Expression {
  case LiteralExpression(literal: Literal)
  case FunctionCallExpression(name: String, arguments: Seq[Expression])
  case Binary(left: Expression, right: Expression, operator: Operator)
  case Unary(expression: Expression)
}

enum Source {
  case StdIn
  case TableRef(identifier: String)
}

case class FromItem(source: Source, joinPredicates: Option[Expression])
case class FromClause(items: NonEmptyList[FromItem])

case class WhereClause(expression: Expression)

enum Statement {

  case SelectStatement(
    projections: NonEmptyList[Expression],
    fromClause: Option[FromClause] = None,
    whereClause: Option[WhereClause] = None
  )

}
