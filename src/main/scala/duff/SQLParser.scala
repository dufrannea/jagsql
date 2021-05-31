package duff

import cats.data.NonEmptyList
import cats.parse.Parser
import cats.implicits._

import scala.math.BigDecimal
import scala.math.exp
import scala.util.matching.Regex
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import duff.CST._
import Expression._
import Statement._
import cats.parse.Parser0

object SQLParser {

  private[this] val whitespace: Parser[Unit] = Parser.charIn(" \t\r\n").void
  private[this] val w: Parser[Unit] = whitespace.rep.void

  case class Keyword(s: String)

  val parser: Parser[String] = Parser.anyChar.rep(10).map {
    _.toList.mkString
  }

  // parsing rules
  def keyword(s: String): Parser[Keyword] =
    Parser
      .string(s)
      .map(_ => Keyword(s))

  val SELECT: Parser[Keyword] = keyword("SELECT")
  val FROM: Parser[Keyword] = keyword("FROM")
  val WHERE: Parser[Keyword] = keyword("WHERE")
  val JOIN: Parser[Keyword] = keyword("JOIN")
  val STDIN = keyword("STDIN")

  val ON = keyword("ON")

  val star: Parser[Unit] = Parser.char('*')

  private val quoteChar = '\''
  val singleQuote: Parser[Unit] = Parser.char(quoteChar)

  val stringLiteral: Parser[Literal.StringLiteral] =
    (singleQuote *> Parser.charWhere(_ != quoteChar).rep0 <* singleQuote)
      .map(k => Literal.string(k.mkString))

  val integer: Parser[String] = Parser
    .charIn((0 to 9).map(_.toString()(0)))
    .rep
    .map(_.toList.mkString)

  val numberLiteral: Parser[Literal.NumberLiteral] =
    (integer ~ (Parser.char('.') *> integer).?)
      .flatMap { case (z, a) =>
        Try {
          BigDecimal(s"$z${a.map(ap => s".$ap").getOrElse("")}")
        } match {
          case Failure(_)     => Parser.failWith(s"Not a number '$z.$a'")
          case Success(value) => Parser.pure(Literal.NumberLiteral(value))
        }
      }

  private val forwardSlashChar = '/'
  private val forwardSlashParser: Parser[Unit] = Parser.char(forwardSlashChar)

  val regexLiteral: Parser[Literal.RegexLiteral] =
    (forwardSlashParser *> Parser
      .charWhere(_ != forwardSlashChar)
      .rep <* forwardSlashParser)
      .flatMap { l =>
        Try {
          new Regex(l.toList.mkString)
          l.toList.mkString
        } match {
          case Failure(_)     => Parser.failWith(s"Not a valid regex ${l.toString}")
          case Success(value) => Parser.pure(Literal.RegexLiteral(value))
        }
      }

  val literal: Parser[Literal] = regexLiteral | stringLiteral | numberLiteral

  val identifier: Parser[String] =
    Parser
      .charIn("abcdefghijklmnopqrstuvwxyz0123456789".toList)
      .rep
      .map(_.toList.mkString)

  val expression: Parser[Expression] = Parser.recursive[Expression] { recurse =>
    val functionCall: Parser[(String, NonEmptyList[Expression])] =
      (identifier <* Parser
        .char('(')) ~ (recurse.rep(1) <* Parser.char(')'))

    literal.map(LiteralExpression.apply) | functionCall.map { case (str, e) =>
      FunctionCallExpression(str, e.toList)
    }
  }

  val ops = List(
    ("=", BinaryOperator.Equal),
    ("!=", BinaryOperator.Different),
    (
      ">=",
      BinaryOperator.MoreEqual
    ),
    (
      "<=",
      BinaryOperator.LessEqual
    ),
    (
      ">",
      BinaryOperator.More
    ),
    (
      "<",
      BinaryOperator.Less
    ),
    (
      "+",
      BinaryOperator.Plus
    ),
    (
      "-",
      BinaryOperator.Minus
    ),
    (
      "*",
      BinaryOperator.Times
    ),
    (
      "/",
      BinaryOperator.Divided
    )
  )

  val binaryOperator = Parser.oneOf(ops.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })

  val start: Parser[Expression] = expression <* w.?
  val end = ((binaryOperator <* w) ~ expression).?

  val binaryExpression = (start ~ end).map {
    case (left, None)              => left
    case (left, Some((op, right))) =>
      Binary(left, right, Operator.B(op))
  }

  val projection: Parser[Expression] = expression

  val tableIdentifier: Parser[Source] =
    STDIN.map(_ => Source.StdIn) | identifier.map(id => Source.TableRef(id))

  val fromClause =
    ((FROM *> w *> (tableIdentifier <* w.?) ~ (JOIN *> w *> (tableIdentifier <* w.?) ~ (ON *> w *> expression))
      .repSep0(0, w)) <* w.?)
      .map { case (source, others) =>
        val tail = others.map { case (source, predicates) =>
          FromItem(source, Some(predicates))
        }
        FromClause(
          NonEmptyList(
            FromItem(source, None),
            tail
          )
        )
      }

  val whereClause = (WHERE *> w *> expression).map(e => WhereClause(e))

  val selectWithProjections =
    (SELECT *> w *> (projection <* w.?).repSep(1, Parser.char(',') <* w.?))
      .map { case (first) =>
        first
      }

  // TODO: can we support only where without FROM ?
  val selectStatement: Parser[Statement] =
    (selectWithProjections ~ (fromClause.?) ~ whereClause.?)
      .map { case ((expressions, maybeFromClause), maybeWhereClause) =>
        SelectStatement(expressions, maybeFromClause, maybeWhereClause)
      }

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

}
