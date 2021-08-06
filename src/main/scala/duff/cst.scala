package duff.jagsql
package cst

import cats.data
import cats.data.NonEmptyList

import java.nio.file.Path
import scala.math.BigDecimal

import Literal.NumberLiteral
import Literal.RegexLiteral
import Literal.StringLiteral
import cst.Projection

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

case class Projection(expression: Expression, maybeAlias: Option[String] = None)

enum Expression {
  case LiteralExpression(literal: Literal)
  case FunctionCallExpression(name: String, arguments: Seq[Expression])
  case Binary(left: Expression, right: Expression, operator: Operator)
  case Unary(expression: Expression)
  case IdentifierExpression(identifier: String)
}

case class Aliased[T](source: T, alias: Option[String])

enum Source {
  case StdIn(alias: String)
  case TableRef(identifier: String, alias: String)
  case SubQuery(statement: Statement.SelectStatement, alias: String)
  case TableFunction(name: String, arguments: List[Literal], alias: String)
}

case class FromSource(source: Source, maybeJoinPredicates: Option[Expression])
case class FromClause(items: NonEmptyList[FromSource])
case class WhereClause(expression: Expression)
case class GroupByClause(expressions: NonEmptyList[Expression])

enum Statement {

  case SelectStatement(
    projections: NonEmptyList[Projection],
    fromClause: Option[FromClause] = None,
    whereClause: Option[WhereClause] = None,
    groupByClause: Option[GroupByClause] = None
  )

}
