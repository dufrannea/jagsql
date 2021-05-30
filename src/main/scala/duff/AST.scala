package duff

import cats.data
import cats.data.NonEmptyList
import duff.AST.Literal.{NumberLiteral, RegexLiteral, StringLiteral}

import java.nio.file.Path
import scala.math.BigDecimal
import duff.AST.FromItem
import duff.AST.FromClause
import duff.AST.Expression
import duff.AST.WhereClause
import duff.AST.Source

object AST {
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


  // Supported operators
  enum BinaryOperator {
    case Equal, Different, Less, More, LessEqual, MoreEqual
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
    // boolean expression necessarily, or coercible to boolean (but we don't like that)
    case Unary(expression: Expression)
  }

  enum Source {
    case StdIn
    case TableRef(identifier: String)
  }

  
  case class FromItem(source: Source, joinPredicates: Option[Expression])
  case class FromClause(items: NonEmptyList[FromItem])
  // object FromClause {
  //   def empty = FromClause(NonEmptyList)
  // }

  case class WhereClause(expression: Expression)

  // enum FromItem {
  //   case First(name: String)
  //   case Other(name: String, predicates: List[Expression])
  // }

  // enum FromClause {
  //   case Empty
  //   case Single(s: FromItem.First)
  //   case Multiple(s: FromItem.First, others: data.NonEmptyList[FromItem.Other])
  // }

  // case class Join(what: String, predicates: List[Expression], others: List[(String, List[Expression])])

  enum Statement {
    case SelectStatement(
      projections: NonEmptyList[Expression],
      fromClause: Option[FromClause] = None,
      whereClause: Option[WhereClause] = None)
  }

}
