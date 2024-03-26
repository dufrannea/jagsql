package duff.jagsql
package parser

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
      Source.TableFunction(name.map(_.toList.mkString), args, alias)
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
