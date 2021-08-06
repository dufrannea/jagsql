package duff.jagsql
package ast

import cats.data.NonEmptyList
import cats.data.NonEmptyMap
import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

import std._
import parser.selectStatement

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

  "SELECT foo.bazzzz FROM (SELECT 1 AS bar) AS foo" fails

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

  }

trait StatementAnalysisDsl extends AnyFreeSpec with Matchers:

  def analyze(e: cst.Statement.SelectStatement): Either[String, ast.Statement.SelectStatement] =
    analyzeStatement(e).runA(Map.empty)

  def analyze(c: String): Either[String, ast.Statement.SelectStatement] =
    analyze(
      selectStatement
        .parseAll(c)
        .map(_.asInstanceOf[cst.Statement.SelectStatement]) match {
        case Left(error)   => sys.error(s"testspec is wrong, cannot parse input, $error")
        case Right(result) => result
      }
    )

  extension (c: cst.Statement.SelectStatement)

    def analyzesTo(expected: ast.Expression): Unit =
      c.toString in {
        analyze(c) match {
          case Left(error)   => fail(error)
          case Right(result) => assert(result == expected)
        }
      }

  extension (c: String) {

    def analyzesTo(expected: ast.Statement.SelectStatement): Unit = {
      import ast._

      c.toString in {
        analyze(c) match {
          case Left(error)   => fail(error)
          case Right(result) => assert(result == expected)
        }
      }
    }

    def hasType(expected: ast.Type): Unit =
      c.toString in {
        analyze(c) match {
          case Left(error)      => fail(error)
          case Right(statement) => assert(statement.tableType == expected)
        }
      }

    def succeeds: Unit =
      c.toString in {
        analyze(c) match {
          case Left(e)       => fail(e)
          case Right(result) => succeed
        }
      }

    def fails: Unit =
      c.toString in {
        analyze(c) match {
          case Left(e)       => succeed
          case Right(result) => fail("expected error")
        }
      }

  }
