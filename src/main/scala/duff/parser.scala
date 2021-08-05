package duff.jagsql

import cats.data.NonEmptyList
import cats.implicits._
import cats.parse.Parser
import cats.parse.Parser0
import cats.syntax.align

import scala.math.BigDecimal
import scala.math.exp
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import cst._
import Expression._
import Statement._
import scala.annotation.internal.Alias

object parser {
  case class Position(from: Int, to: Int)

  def indexed[T](p: Parser[T]): Parser[(T, Position)] = (Parser.index.with1 ~ p ~ Parser.index).map {
    case ((from, what), to) => (what, Position(from, to))
  }

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

  // I need to watch tpolecat's presentation on cofree to embed the things in his parser
  //  - IndexedParser with positions ?
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

  val boolLiteral: Parser[Literal] = Parser.string("true").map(_ => Literal.BoolLiteral(true)) | Parser
    .string("false")
    .map(_ => Literal.BoolLiteral(false))

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

  val literal: Parser[Literal] = regexLiteral | stringLiteral | numberLiteral | boolLiteral

  val identifier = Parser
    .charIn("abcdefghijklmnopqrstuvwxyz0123456789_".toList)
    .rep

  val upperCaseIdentifier = Parser
    .charIn("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toList)
    .rep

  val compositeIdentifier: Parser[String] = {
    (identifier ~ (Parser.char('.') *> identifier).?).map { case (left, maybeRight) =>
      left.toList.mkString + (maybeRight match {
        case None        => ""
        case Some(chars) => "." + chars.toList.mkString
      })
    }

  }

  val orAnd = List(
    (
      "||",
      BinaryOperator.Or
    ),
    (
      "&&",
      BinaryOperator.And
    )
  )

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
    )
  )

  val timesDivided = List(
    (
      "*",
      BinaryOperator.Times
    ),
    (
      "/",
      BinaryOperator.Divided
    )
  )

  val plusMinus = List(
    (
      "+",
      BinaryOperator.Plus
    ),
    (
      "-",
      BinaryOperator.Minus
    )
  )

  val prioBinaryOperator = Parser.oneOf(orAnd.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })

  val binaryOperator = Parser.oneOf(ops.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })

  val plusMinusOperator = Parser.oneOf(plusMinus.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })

  val timesDividedOperator = Parser.oneOf(timesDivided.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })

  def binaryExpression(operator: Parser[BinaryOperator])(inner: Parser[Expression]) = {
    ((inner <* w.?) ~ (operator ~ (w.? *> inner <* w.?)).rep0).map { case (left, rightOperands) =>
      rightOperands.foldLeft(left) { case (acc, (op, current)) =>
        Binary(acc, current, Operator.B(op))
      }
    }
  }

  val expression: Parser[Expression] = Parser.recursive[Expression] { recurse =>
    val functionCall: Parser[(String, NonEmptyList[Expression])] =
      (compositeIdentifier <* Parser
        .char('(')) ~ (recurse.rep(1) <* Parser.char(')'))

    // backtracking on function call because of the ambiguity of the single
    // identifier vs identifier + parens for the functions
    // we can avoid backtracking by creating a functionOrIdentifier parser
    // TODO: this ^
    val term = literal.map(LiteralExpression.apply) | functionCall.map { case (str, e) =>
      FunctionCallExpression(str, e.toList)
    }.backtrack | (Parser.char('(') *> w.? *> recurse <* w.? *> Parser.char(')')) | compositeIdentifier.map(
      IdentifierExpression.apply
    )

    val lowPriority = binaryExpression(binaryOperator)(term)
    val prioBinary = binaryExpression(prioBinaryOperator)(lowPriority)
    val times = binaryExpression(timesDividedOperator)(prioBinary)
    val plus = binaryExpression(plusMinusOperator)(times)
    plus
  }

  val projection: Parser[Projection] = (expression ~ (keyword("AS") *> w *> compositeIdentifier <* w.?).?).map {
    case (e, maybeAlias) =>
      Projection(e, maybeAlias)
  }

  val whereClause = (WHERE *> w *> expression).map(e => WhereClause(e))

  def maybeAliased[T](parser: Parser[T]): Parser[(T, Option[String])] =
    parser ~ (w *> keyword("AS") *> w *> compositeIdentifier).backtrack.?

  def aliased[T](parser: Parser[T]): Parser[(T, String)] =
    parser ~ (w *> keyword("AS") *> w *> compositeIdentifier)

  // TODO: can we support only where without FROM
  val selectStatement: Parser[SelectStatement] = Parser.recursive { recurse =>

    val parensSub = Parser.char('(') *> w.? *> recurse <* w.? *> Parser.char(')')

    val tableFunction = (upperCaseIdentifier <* Parser.char('(') <* w.?) ~
      (literal <* w.?).repSep0(Parser.char(',') <* w.?) <* Parser.char(')')

    val subQuery = parensSub

    val selectSource: Parser[Source] =
      // Tableref is for when CTE will be supported, currently there is no way to
      // refer to a subquery in another join clause
      maybeAliased(STDIN).map { case (stdin, alias) => Source.StdIn(alias.getOrElse("in")) }
        | maybeAliased(compositeIdentifier).map { case (tableRef, alias) =>
          Source.TableRef(tableRef, alias.getOrElse(tableRef))
        }
        | aliased(subQuery).map { case (statement, alias) => Source.SubQuery(statement, alias) }
        | aliased(tableFunction).map { case ((name, args), alias) =>
          Source.TableFunction(name.toList.mkString, args.toList, alias)
        }

    val joinClause = (JOIN *> w *> (selectSource <* w.?) ~ (ON *> w *> expression <* w.?)).rep(1)

    val fromClause =
      (FROM *> w *> (selectSource <* w.?) ~ joinClause.?)
        .map { case (source, o) =>
          val others = o.toList.flatMap(_.toList)
          val tail = others.map { case (source, predicates) =>
            FromSource(source, Some(predicates))
          }
          FromClause(
            NonEmptyList(
              FromSource(source, None),
              tail
            )
          )
        }

    val selectWithProjections =
      (SELECT *> w *> (projection <* w.?).repSep(1, Parser.char(',') <* w.?))
        .map { case (first) =>
          first
        }

    (selectWithProjections ~ (fromClause.?) ~ whereClause.?)
      .map { case ((expressions, maybeFromClause), maybeWhereClause) =>
        SelectStatement(expressions, maybeFromClause, maybeWhereClause)
      }

  }

  def parse(query: String): Either[String, SelectStatement] = {
    def formatError(e: Parser.Error): String = e match {
      case Parser.Error(pos, _) =>
        (1 to pos).map(_ => " ").mkString + "^"
    }
    selectStatement.parseAll(query) match {
      case Left(error) => Left("In query :\n" + query + "\n" + formatError(error) + "\n" + error.toString)

      case r @ Right(result) => Right(result)
    }
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

export parser.selectStatement
export parser.literal
