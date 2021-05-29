package duff

import scala.math.BigDecimal

object AST {
  sealed trait Literal
  case class StringLiteral(a: String) extends Literal
  case class NumberLiteral(a: BigDecimal) extends Literal
  case class RegexLiteral(str: String) extends Literal

  object Literal {
    def string(s: String): StringLiteral = StringLiteral(s)
    def int(s: BigDecimal): NumberLiteral = NumberLiteral(s)
    def regex(s: String): RegexLiteral = RegexLiteral(s)
  }

  sealed trait Expression
  case class LiteralExpression(literal: Literal) extends Expression
  case class FunctionCallExpression(name: String, arguments: Seq[Expression]) extends Expression

}
