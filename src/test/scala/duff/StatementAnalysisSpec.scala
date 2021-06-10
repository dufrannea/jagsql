package duff
package ast

import utils._
import SQLParser.selectStatement

import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import cats.data.NonEmptyList
import cats.data.NonEmptyMap

import scala.language.postfixOps

class StatementAnalysisSpec extends StatementAnalysisDsl:

  val one: Expression = Fix(ExpressionF.LiteralExpression(cst.Literal.NumberLiteral(BigDecimal(1)), Type.Number))

  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("0" -> SimpleType.Number))

  "SELECT 1 FROM STDIN" analyzesTo {
    Statement.SelectStatement(
      NonEmptyList.one(Projection(one, None)),
      FromClause(NonEmptyList.of(FromSource(Source.StdIn, None))),
      None,
      ss
    )
  }

  "SELECT 1" analyzesTo {
    Statement.SelectStatement(
      NonEmptyList.one(Projection(one, None)),
      FromClause(NonEmptyList.of(FromSource(Source.StdIn, None))),
      None,
      ss
    )
  }

  "SELECT foo.bar FROM (SELECT 1 AS bar) AS foo" succeeds

  "SELECT foo.bar FROM (SELECT 1 AS bar) AS foo JOIN (SELECT 2 AS bar) AS baz ON foo.bar = baz.bar" succeeds

  "SELECT foo.bazzzz FROM (SELECT 1 AS bar) AS foo" fails


trait StatementAnalysisDsl extends AnyFreeSpec with Matchers:

  def analyze(e: cst.Statement.SelectStatement): Either[String, ast.Statement.SelectStatement] =
    analyzeStatement(e).runA(Map.empty)

  def analyze(c: String): Either[String, ast.Statement.SelectStatement] =
    analyze(
      selectStatement
        .parseAll(c)
        .map(_.asInstanceOf[cst.Statement.SelectStatement])
        .getOrElse(sys.error("testspec is wrong, cannot parse input"))
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
