package duff.jagsql
package cst

import duff.jagsql.cst.Literal.{NumberLiteral, RegexLiteral, StringLiteral}
import duff.jagsql.cst.Projection

import java.nio.file.Path

import scala.math.BigDecimal

import cats.data
import cats.data.NonEmptyList

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

enum BinaryOperator extends Operator {
  case Equal, Different, Less, More, LessEqual, MoreEqual, Plus, Minus, Times, Divided, Or, And
}

enum UnaryOperator extends Operator {
  case Not
}

sealed trait Operator

object Operator {
  val Equal = BinaryOperator.Equal
  val Different = BinaryOperator.Different
  val Less = BinaryOperator.Less
  val More = BinaryOperator.More
  val LessEqual = BinaryOperator.LessEqual
  val MoreEqual = BinaryOperator.MoreEqual
  val Plus = BinaryOperator.Plus
  val Minus = BinaryOperator.Minus
  val Times = BinaryOperator.Times
  val Divided = BinaryOperator.Divided
  val Or = BinaryOperator.Or
  val And = BinaryOperator.And
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
