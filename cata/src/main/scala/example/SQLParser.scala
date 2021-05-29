package example

import cats.parse._
import cats.instances.int

object SQLParser extends App {

  sealed trait HiveType
  case object HiveInt extends HiveType
  case object HiveString extends HiveType

  case class Keyword(s: String)

  case class Literal(repr: String, t: HiveType)
  object Literal {
    def string(s: String) = Literal(s, HiveString)
    def int(s: String) = Literal(s, HiveInt)
  }
  val parser: Parser[String] = Parser.anyChar.rep(10).map {
    _.toList.mkString
  }

  // parsing rules
  def keyword(s: String): Parser[Keyword] =
    Parser
      .string(s)
      .map(_ => Keyword(s))

  val SELECT = keyword("SELECT")
  val FROM = keyword("FROM")
  val WHERE = keyword("SELECT")

  val star = Parser.char('*')

  val singleQuote = Parser.char('\'')
  val stringLiteral =
    Parser.anyChar.rep
      .surroundedBy(singleQuote)
      .map(k => Literal.string(k.toList.mkString))

  val intLiteral = Parser
    .charIn((0 to 9).map(_.toString()(0)))
    .rep
    .map(l => {
      Literal.int(l.toList.mkString)
    })

  // val literal = Parser.oneOf(stringLiteral :: intLiteral :: Nil).
  // val chars = (0 until 25).map('a' + _).map(_.toChar)

  // sealed trait Expression
  // case class LiteralExpression(l: Literal)

  // val expression = Parser.recursive(p => {

  // })
  // println(chars)
  // println((singleQuote ~ Parser.ignoreCaseCharIn(chars).rep ~ singleQuote).parse("'ab'"))

}
