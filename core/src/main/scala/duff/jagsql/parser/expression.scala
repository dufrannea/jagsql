package duff.jagsql
package parser

import duff.jagsql.cst.{BinaryOperator, Expression, Literal, UnaryOperator}
import duff.jagsql.cst.Expression.{Binary, FunctionCallExpression, IdentifierExpression, LiteralExpression, Unary}
import duff.jagsql.std.RegexWrapper

import scala.math.BigDecimal
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

import cats.*
import cats.Functor
import cats.data.NonEmptyList
import cats.implicits.*
import cats.parse.Parser

private val quoteChar = '\''
val singleQuote: Parser[Unit] = Parser.char(quoteChar)

val stringLiteral: Parser[Literal.StringLiteral] =
  (singleQuote *> Parser.charWhere(_ != quoteChar).rep0 <* singleQuote)
    .map(k => Literal.string(k.mkString))

val numberLiteral: Parser[Literal.NumberLiteral] = {
  val integer: Parser[String] = Parser
    .charIn((0 to 9).map(_.toString()(0)))
    .rep
    .map(_.toList.mkString)

  val signedInteger: Parser[String] = (Parser.char('-') *> integer).map(s => s"-$s") | integer

  (signedInteger ~ (Parser.char('.') *> integer).?)
    .flatMap { case (z, a) =>
      Try {
        BigDecimal(s"$z${a.map(ap => s".$ap").getOrElse("")}")
      } match {
        case Failure(_)     => Parser.failWith(s"Not a number '$z.$a'")
        case Success(value) => Parser.pure(Literal.NumberLiteral(value))
      }
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
      RegexWrapper.build(l.toList.mkString) match {
        case Failure(_)     => Parser.failWith(s"Not a valid regex ${l.toString}")
        case Success(value) => Parser.pure(Literal.RegexLiteral(value))
      }
    }

val literal: IndexedParser[Literal] = (regexLiteral | stringLiteral | numberLiteral | boolLiteral).indexed

val identifier = Parser
  .charIn("abcdefghijklmnopqrstuvwxyz0123456789_".toList)
  .rep
  .indexed

val upperCaseIdentifier = Parser
  .charIn("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toList)
  .rep
  .indexed

val compositeIdentifier: IndexedParser[String] =
  (identifier.unindexed ~ (Parser.char('.') *> identifier.unindexed).?).map { case (left, maybeRight) =>
    left.toList.mkString + (maybeRight match {
      case None        => ""
      case Some(chars) => "." + chars.toList.mkString
    })
  }.indexed

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

val prioBinaryOperator = Parser
  .oneOf(orAnd.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })
  .indexed

val unaryOperator: IndexedParser[UnaryOperator] = Parser.char('!').as(UnaryOperator.Not).indexed

val binaryOperator = Parser
  .oneOf(ops.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })
  .indexed

val plusMinusOperator = Parser
  .oneOf(plusMinus.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })
  .indexed

val timesDividedOperator = Parser
  .oneOf(timesDivided.map { case (s, op) =>
    Parser.string(s).map(_ => op)
  })
  .indexed

// weird:
// Are the indices of the Binary right here ?
def binaryExpression(
  operator: Parser[Indexed[BinaryOperator]]
)(
  inner: IndexedParser[Expression[Indexed]]
): IndexedParser[Expression[Indexed]] =
  ((inner <* w.?) ~ (operator ~ (w.? *> inner <* w.?)).rep0).map { case (left, rightOperands) =>
    rightOperands.foldLeft(left) { case (acc, (op, current)) =>
      (acc + current).map(_ => Binary(acc, current, op))
    }
  }

def unaryExpression(
  operator: Parser[Indexed[UnaryOperator]]
)(
  inner: IndexedParser[Expression[Indexed]]
): IndexedParser[Expression[Indexed]] =
  (operator ~ (w.? *> inner <* w.?)).map { case (op, in) =>
    Unary(in, op)
  }.indexed

def comaSeparatedWithSpace[K](p: IndexedParser[K]): Parser[NonEmptyList[Indexed[K]]] =
  (p <* w.?).repSep(Parser.char(',') <* w.?)

def comaSeparatedBetweenParens[K](p: IndexedParser[K]): Parser[NonEmptyList[Indexed[K]]] =
  Parser.char('(') *> w.? *> comaSeparatedWithSpace(p) <* w.? <* Parser.char(')')

def maybeSubscripted(s: IndexedParser[Expression[Indexed]]): IndexedParser[Expression[Indexed]] =
  (s ~ (Parser.char('.') *> compositeIdentifier).?).map {
    case (e, None)             => e.value
    case (e, Some(identifier)) =>
      cst
        .Expression
        .StructAccess(
          e,
          identifier.map { (k: String) =>
            k.split("\\.").toList.toNel.getOrElse(sys.error("Identifier should be guaranteed non empty at this stage"))
          }
        )
  }.indexed

val expression: Parser[Indexed[Expression[Indexed]]] = Parser.recursive[Indexed[Expression[Indexed]]] { recurse =>
  val functionCall: IndexedParser[(Indexed[String], NonEmptyList[Indexed[Expression[Indexed]]])] =
    (compositeIdentifier
      ~ comaSeparatedBetweenParens(recurse)).indexed

  // backtracking on function call because of the ambiguity of the single
  // identifier vs identifier + parens for the functions
  // we can avoid backtracking by creating a functionOrIdentifier parser
  // TODO: this ^
  val term: IndexedParser[Expression[Indexed]] = {
    val functionTerm: IndexedParser[FunctionCallExpression[Indexed]] =
      Functor[IndexedParser].map(functionCall.backtrack) { case (str, e) =>
        FunctionCallExpression(str, e.toList)
      }
    val literalTerm: IndexedParser[LiteralExpression[Indexed]] = literal.emap(LiteralExpression.apply)

    literalTerm | functionTerm | (Parser.char('(') *> w.? *> recurse <* w.? *> Parser.char(')')) | compositeIdentifier
      .emap(
        IdentifierExpression.apply
      ) | unaryExpression(unaryOperator)(recurse)
  }

  val z = maybeSubscripted(term).backtrack | term

  val lowPriority = binaryExpression(binaryOperator)(z)
  val prioBinary = binaryExpression(prioBinaryOperator)(lowPriority)
  val times = binaryExpression(timesDividedOperator)(prioBinary)
  val plus = binaryExpression(plusMinusOperator)(times)
  plus
}
