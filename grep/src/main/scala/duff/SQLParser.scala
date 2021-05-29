package duff

import cats.data.NonEmptyList
import cats.parse.Parser

import scala.math.BigDecimal
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object SQLParser {
  import duff.AST._

  case class Keyword(s: String)

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
  val WHERE = keyword("WHERE")

  val star = Parser.char('*')

  private val quoteChar = '\''
  val singleQuote: Parser[Unit] = Parser.char(quoteChar)

  val stringLiteral: Parser[StringLiteral] =
    (singleQuote *> Parser.charWhere(_ != quoteChar).rep0 <* singleQuote)
      .map(k => Literal.string(k.mkString))

  val integer: Parser[String] = Parser
    .charIn((0 to 9).map(_.toString()(0)))
    .rep
    .map(_.toList.mkString)

  val numberLiteral: Parser[NumberLiteral] = (integer ~ (Parser.char('.') *> integer).?)
    .flatMap({
      case (z, a) =>
        Try {
          BigDecimal(s"$z${a.map(ap => s".$ap").getOrElse("")}")
        } match {
          case Failure(_)     => Parser.failWith(s"Not a number '$z.$a'")
          case Success(value) => Parser.pure(NumberLiteral(value))
        }
    })

  private val forwardSlashChar = '/'
  val forwardSlashParser: Parser[Unit] = Parser.char(forwardSlashChar)

  val regexLiteral: Parser[RegexLiteral] =
    (forwardSlashParser *> Parser.charWhere(_ != forwardSlashChar).rep <* forwardSlashParser)
      .flatMap(l => {
        Try {
          new Regex(l.toList.mkString)
          l.toList.mkString
        } match {
          case Failure(_)     => Parser.failWith(s"Not a valid regex ${l.toString}")
          case Success(value) => Parser.pure(RegexLiteral(value))
        }
      })

  val literal: Parser[Literal] = regexLiteral | stringLiteral | numberLiteral

  val identifier: Parser[String] =
    Parser.charIn("abcdefghijklmnopqrstuvwxyz0123456789".toList).rep.map(_.toList.mkString)

  val expression: Parser[Expression] = Parser.recursive[Expression](recurse => {
    val functionCall: Parser[(String, NonEmptyList[Expression])] = (identifier <* Parser
      .char('(')) ~ (recurse.rep(1) <* Parser.char(')'))

    literal.map(LiteralExpression) | functionCall.map {
      case (str, e) => FunctionCallExpression(str, e.toList)
    }
  })

  val projection = literal

  val selectStatement: Parser[Literal] = SELECT *> Parser
    .char(' ')
    .rep *> projection //(Parser.char(','))

  // Arithmetic operations
  // Group expansion of regexes with several groups (or better syntax, maybe glob ? maybe cut ?)
  // simpler split operator probably
  // transtyping to json / csv / other
  // running bash code ?
  // Running the queries
  // - group by
  // - joins
  // Crazy idea, bake into scala or at least ammonite would be awesome, to have it into a repl
  //
  // Interesting to have like MDX queries running to get stats and stuff super easily
  // maybe look into prometheus syntax, can be interesting
  //
  // analyses regexes to understand the structure of the table
  def main(args: Array[String]): Unit =
    println(selectStatement.parse("SELECT /lol/"))

  //
//  class ParserOps[T](parser: Parser[T]) {
//    def apply(thunk : => ()) = {
//
//    }
//  }
  // val literal = Parser.oneOf(stringLiteral :: intLiteral :: Nil).
  // val chars = (0 until 25).map('a' + _).map(_.toChar)

  // sealed trait Expression
  // case class LiteralExpression(l: Literal)

  // val expression = Parser.recursive(p => {

  // })
  // println(chars)
  // println((singleQuote ~ Parser.ignoreCaseCharIn(chars).rep ~ singleQuote).parse("'ab'"))

}
