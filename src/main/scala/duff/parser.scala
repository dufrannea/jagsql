package duff.jagsql

import duff.jagsql.cst.*
import duff.jagsql.cst.Expression.*
import duff.jagsql.cst.Statement.*

import scala.annotation.internal.Alias
import scala.math.{exp, BigDecimal}
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

import cats.CoflatMap
import cats.Functor
import cats.data.NonEmptyList
import cats.implicits.*
import cats.parse.{Parser, Parser0}
import cats.syntax.align

object parser {

  type IndexedParser[T] = Parser[Indexed[T]]

  given indexedParserFunctor: Functor[IndexedParser] = new Functor[IndexedParser] {

    def map[A, B](fa: IndexedParser[A])(f: A => B): IndexedParser[B] = fa.map { case Indexed(value, pos) =>
      Indexed(f(value), pos)
    }

  }

  extension [K](p: IndexedParser[K]) {

    def unindexed: Parser[K] = p.map(_.value)

    //
    // F[A] -> (F[A] ->  B) => F[B]
    // that's *not* comonad, is it were f would need to be IndexedParser => B ?
    def emap[B](f: Indexed[K] => B): IndexedParser[B] = p.map { case v @ Indexed(value, pos) =>
      Indexed(f(v), pos)
    }

  }

  extension [T](p: Parser[T]) {

    def indexed: IndexedParser[T] = (Parser.index.with1 ~ p ~ Parser.index).map { case ((from, what), to) =>
      Indexed(what, Position(from, to))
    }

  }

  private[this] val whitespace: Parser[Unit] = Parser.charIn(" \t\r\n").void
  private[this] val w: Parser[Unit] = whitespace.rep.void

  case class Keyword(s: String)

  // parsing rules
  def keyword(s: String) =
    Parser
      .string(s)
      .map(_ => Keyword(s))
      .indexed

  // I need to watch tpolecat's presentation on cofree to embed the things in his parser
  //  - IndexedParser with positions ?
  val SELECT: IndexedParser[Keyword] = keyword("SELECT")
  val FROM: IndexedParser[Keyword] = keyword("FROM")
  val WHERE: IndexedParser[Keyword] = keyword("WHERE")
  val JOIN: IndexedParser[Keyword] = keyword("JOIN")
  val STDIN = keyword("STDIN")
  val DUAL = keyword("DUAL")

  val ON = keyword("ON")

  val star = Parser.char('*').indexed

  private val quoteChar = '\''
  val singleQuote: Parser[Unit] = Parser.char(quoteChar)

  val stringLiteral: Parser[Literal.StringLiteral] =
    (singleQuote *> Parser.charWhere(_ != quoteChar).rep0 <* singleQuote)
      .map(k => Literal.string(k.mkString))

  val diider = 1

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

  def comaSeparatedWithSpace[K](p: IndexedParser[K]): Parser[NonEmptyList[Indexed[K]]] =
    (p <* w.?).repSep(Parser.char(',') <* w.?)

  def comaSeparatedBetweenParens[K](p: IndexedParser[K]): Parser[NonEmptyList[Indexed[K]]] =
    Parser.char('(') *> w.? *> comaSeparatedWithSpace(p) <* w.? <* Parser.char(')')

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
        )
    }

    val lowPriority = binaryExpression(binaryOperator)(term)
    val prioBinary = binaryExpression(prioBinaryOperator)(lowPriority)
    val times = binaryExpression(timesDividedOperator)(prioBinary)
    val plus = binaryExpression(plusMinusOperator)(times)
    plus
  }

  val projection: IndexedParser[Projection[Indexed]] =
    ((expression ~ (keyword("AS") *> w *> compositeIdentifier).?).indexed <* w.?).map {
      case Indexed((e, maybeAlias), pos) =>
        Indexed(Projection(e, maybeAlias), pos)
    }

  val whereClause = (WHERE *> w *> expression).map(e => WhereClause(e)).indexed

  def maybeAliased[T](parser: IndexedParser[T]): IndexedParser[(Indexed[T], Option[Indexed[String]])] =
    (parser ~ (w *> keyword("AS") *> w *> compositeIdentifier).backtrack.?).indexed

  def aliased[T](parser: IndexedParser[T]): IndexedParser[(Indexed[T], Indexed[String])] =
    (parser ~ (w *> keyword("AS") *> w *> compositeIdentifier)).indexed

//  def aliased[T](parser: Parser[T]): Parser[(T, Indexed[String])] =
//    parser ~ (w *> keyword("AS") *> w *> compositeIdentifier)
//
  // TODO: can we support only where without FROM
  val selectStatement: IndexedParser[SelectStatement[Indexed]] = Parser
    .recursive[Indexed[SelectStatement[Indexed]]] { recurse =>

      val parensSub: IndexedParser[SelectStatement[Indexed]] =
        Parser.char('(') *> w.? *> recurse <* w.? *> Parser.char(')')

      val zop: IndexedParser[(Indexed[NonEmptyList[Char]], List[Indexed[Literal]])] =
        ((upperCaseIdentifier <* Parser.char('(') <* w.?) ~
          (literal <* w.?).repSep0(Parser.char(',') <* w.?) <* Parser.char(')')).indexed

      val tableFunction: IndexedParser[Source.TableFunction[Indexed]] = Functor[IndexedParser].map(
        aliased(zop)
      ) { case (Indexed((name, args), p), alias) =>
        Source.TableFunction(name.map(_.toList.mkString), args.toList, alias)
      }

      val aliasedTableFunction = aliased(tableFunction)

      val subQuery = parensSub

      val selectSource: IndexedParser[Source[Indexed]] =
        // Tableref is for when CTE will be supported, currently there is no way to
        // refer to a subquery in another join clause
        // TODO: aliasing should not be handled in the parser
        Functor[IndexedParser].map(maybeAliased(STDIN | DUAL)) {
          case (Indexed(Keyword("STDIN"), pos), alias) => Source.StdIn(alias.getOrElse(Indexed("in", pos)))
          case (Indexed(Keyword("DUAL"), pos), alias)  => Source.Dual(alias.getOrElse(Indexed("in", pos)))
          case _                                       => sys.error("Non supported source")
        }
          | Functor[IndexedParser].map(maybeAliased(compositeIdentifier)) { case (tableRef, alias) =>
            Source.TableRef(tableRef, alias.getOrElse(tableRef))
          }
          | Functor[IndexedParser].map(aliased(subQuery)) { case (statement, alias) =>
            Source.SubQuery(statement, alias)
          } | tableFunction

      val joinClause: Parser[NonEmptyList[(Indexed[Source[Indexed]], Indexed[Expression[Indexed]])]] =
        (JOIN *> w *> (selectSource <* w.?) ~ (ON *> w *> expression <* w.?)).rep(1)

      val fromClause: IndexedParser[FromClause[Indexed]] =
        Functor[IndexedParser].map((FROM *> w *> (selectSource <* w.?) ~ joinClause.?).indexed) {
          case (source, maybeJoinClause) =>
            val others: List[(Indexed[Source[Indexed]], Indexed[Expression[Indexed]])] =
              maybeJoinClause.map(_.toList).getOrElse(Nil)
            val tail: List[Indexed[FromSource[Indexed]]] = others.map { case (source, predicate) =>
              (source + predicate).as {
                FromSource(source, Some(predicate))
              }
            }
            // here we should span the whole thing
            FromClause(
              NonEmptyList(
                source.coflatMap(FromSource(_, None)),
                tail
              )
            )
        }

      val selectWithProjections =
        (SELECT *> w *> (projection <* w.?).repSep(1, Parser.char(',') <* w.?))
          .map { case (first) =>
            first
          }

      val groupBy = (keyword("GROUP BY") *> w *> (expression <* w.?).repSep(1, Parser.char(',') <* w.?)).map {
        case expressions => GroupByClause(expressions)
      }.indexed

      val r: Parser[SelectStatement[Indexed]] =
        (selectWithProjections ~ (fromClause.?) ~ whereClause.? ~ groupBy.?).map {
          case (((expressions, maybeFromClause), maybeWhereClause), maybeGroupByClause) =>
            SelectStatement(expressions, maybeFromClause, maybeWhereClause, maybeGroupByClause)
        }

      r.indexed

    }

  def parse(query: String): Either[String, SelectStatement[Indexed]] = {
    def formatError(e: Parser.Error): String = e match {
      case Parser.Error(pos, _) =>
        (1 to pos).map(_ => " ").mkString + "^"
    }

    selectStatement.parseAll(query) match {
      case Left(error) => Left("In query :\n" + query + "\n" + formatError(error) + "\n" + error.toString)

      case r @ Right(result) => Right(result.value)
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

export parser.literal
export parser.selectStatement
