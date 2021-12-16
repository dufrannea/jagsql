package duff.jagsql
package ast

import duff.jagsql.ast.validation.*
import duff.jagsql.cst.Indexed
import duff.jagsql.parser.selectStatement
import duff.jagsql.std.*

import scala.language.postfixOps

import cats.data.{NonEmptyList, NonEmptyMap}

import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class StatementAnalysisSpec extends StatementAnalysisDsl:

  val one: Expression = Fix(ExpressionF.LiteralExpression(cst.Literal.NumberLiteral(BigDecimal(1)), Type.Number))

  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.Number))

  "SELECT 1 FROM STDIN" analyzesTo {
    Statement.SelectStatement(
      NonEmptyList.one(Projection(one, None)),
      FromClause(NonEmptyList.of(FromSource(Source.StdIn("in"), None))),
      None,
      None,
      ss
    )
  }

  "SELECT 1" analyzesTo {
    Statement.SelectStatement(
      NonEmptyList.one(Projection(one, None)),
      FromClause(NonEmptyList.of(FromSource(Source.StdIn("in"), None))),
      None,
      None,
      ss
    )
  }

  "SELECT foo.bar FROM (SELECT 1 AS bar) AS foo" succeeds

  "SELECT lol.col_0 FROM STDIN AS lol" succeeds

  "SELECT foo.bar FROM (SELECT 1 AS bar) AS foo JOIN (SELECT 2 AS bar) AS baz ON foo.bar = baz.bar" succeeds

  "SELECT foo.bazzzz FROM (SELECT 1 AS bar) AS foo" failsWith {
    """ERROR: Non existing field bazzzz on table foo
      |
      |SELECT foo.bazzzz FROM (SELECT 1 AS bar) AS foo
      |       ^^^^^^^^^^""".stripMargin
  }

  "SELECT foo.col_0 FROM STDIN AS foo GROUP BY foo.col_0" succeeds

  // group by non existing column fails
  "SELECT 1 FROM (SELECT 1 AS foo, 2 AS bar) AS foo GROUP BY foo.non_existing_col" fails

  "SELECT foo.bar FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar" succeeds

  "Cannot select a field if not in group or not in an aggregate function" - {
    "SELECT foo.bar, foo.baz FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar" fails

    "SELECT foo.bar, max(foo.baz) FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar" succeeds

    "SELECT max(foo.baz) FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar" succeeds

    "SELECT foo.bar + 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar + 1" succeeds

    "SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar" succeeds

    "SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY max(foo.bar)" fails

    "SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar, max(foo.bar)" fails

    "SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar, array(foo.bar)" succeeds

    "SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar, array(max(foo.bar))" failsWith {
      """ERROR: Expressions in GROUP BY clause should not contain aggregation functions
        |
        |SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar, array(max(foo.bar))
        |                                                                         ^^^^^^^^^^^^""".stripMargin
    }

    """|SELECT
       |  1 AS one
       |  ,1 * 42 AS three
       |  ,a.b + 1 AS four
       |  ,a.d -1 AS five
       |  ,a.d -1 + 1 AS six
       |  ,max(a.c - 1) + 1 AS seven
       |FROM
       |  (SELECT 1 AS b, 1 AS d, 1 AS c) AS a
       |GROUP BY a.b, a.d - 1""".stripMargin succeeds

    """|SELECT
       |  a.d AS one
       |FROM
       |  (SELECT 1 AS b, 1 AS d, 1 AS c) AS a
       |GROUP BY a.d - 1""".stripMargin failsWith {
      """ERROR: Expression is not included in GROUP BY
        |
        |SELECT
        |         
        |  a.d AS one
        |  ^^^""".stripMargin
    }

    """|SELECT
       |  a.d - 1 AS one
       |FROM
       |  (SELECT 1 AS b, 1 AS d, 1 AS c) AS a
       |GROUP BY a.d - 1""".stripMargin succeeds
  }

trait StatementAnalysisDsl extends AnyFreeSpec with Matchers:

  def analyze(e: cst.Statement.SelectStatement[Indexed]): Either[TrackedError, ast.Statement.SelectStatement] =
    analyzeStatement(e).runA(Map.empty)

  def analyze(c: String): Either[TrackedError, ast.Statement.SelectStatement] =
    analyze(
      selectStatement
        .parseAll(c) match {
        case Left(error)   => sys.error(s"testspec is wrong, cannot parse input, $error")
        case Right(result) => result.value
      }
    )

  extension (c: String) {

    def analyzesTo(expected: ast.Statement.SelectStatement): Unit = {
      import ast.*

      c in {
        analyze(c) match {
          case Left(error)   => fail(error.format(c))
          case Right(result) => assert(result == expected)
        }
      }
    }

    def hasType(expected: ast.Type): Unit =
      c in {
        analyze(c) match {
          case Left(error)      => fail(error.format(c))
          case Right(statement) => assert(statement.tableType == expected)
        }
      }

    def succeeds: Unit =
      c in {
        analyze(c) match {
          case Left(e)       => fail(e.format(c))
          case Right(result) => succeed
        }
      }

    def fails: Unit =
      c in {
        analyze(c) match {
          case Left(_)       => succeed
          case Right(result) => fail("expected error")
        }
      }

    def failsWith(s: String): Unit =
      c in {
        analyze(c) match {
          case Left(e)       =>
            val formattedError = e.format(c)
            assert(formattedError == s)
          case Right(result) => fail("expected error")
        }
      }

  }
