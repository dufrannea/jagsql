package duff

import cats.data
import cats.data.NonEmptyList
import duff.CST.Literal.NumberLiteral
import duff.CST.Literal.RegexLiteral
import duff.CST.Literal.StringLiteral

import java.nio.file.Path
import scala.math.BigDecimal
import duff.CST.FromItem
import duff.CST.FromClause
import duff.CST.Expression
import duff.CST.WhereClause
import duff.CST.Source

object CST {

  enum Literal {
    case StringLiteral(a: String)
    case NumberLiteral(a: BigDecimal)
    case RegexLiteral(str: String)
    case FileLiteral(path: Path)
  }

  object Literal {
    def string(s: String): StringLiteral = StringLiteral(s)
    def int(s: BigDecimal): NumberLiteral = NumberLiteral(s)
    def regex(s: String): RegexLiteral = RegexLiteral(s)
  }

  enum BinaryOperator {
    case Equal, Different, Less, More, LessEqual, MoreEqual, Plus, Minus, Times, Divided
  }

  enum UnaryOperator {
    case Not
  }

  enum Operator {
    case B(op: BinaryOperator)
    case U(op: UnaryOperator)
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

}
