package duff.jagsql
package ast

import duff.jagsql.ast.validation.*
import duff.jagsql.cst.{Expression, Indexed, Literal, Position}
import duff.jagsql.parser.expression
import duff.jagsql.std.*

import scala.language.postfixOps

import cats.data.State

import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ExpressionAnalysisSpec extends AnalysisDsl:

  val one = Literal.NumberLiteral(BigDecimal(1))
  val oneIndexed = Indexed(Literal.NumberLiteral(BigDecimal(1)), Position.empty)
  val someString = Literal.StringLiteral("someString")
  val someStringIndexed = Indexed(Literal.StringLiteral("someString"), Position.empty)

  Expression.LiteralExpression[Indexed](oneIndexed) analyzesTo
    Fix(
      ast
        .ExpressionF
        .LiteralExpression(one, ast.Type.Number)
    )

  Expression.LiteralExpression[Indexed](someStringIndexed) analyzesTo
    Fix(
      ast
        .ExpressionF
        .LiteralExpression(someString, ast.Type.String)
    )

  "1 + 1" analyzesTo {
    val left = Fix(
      ast
        .ExpressionF
        .LiteralExpression(one, ast.Type.Number)
    )

    val ef = ast
      .ExpressionF
      .Binary(left, left, cst.Operator.Plus, ast.Type.Number)
    Fix(ef)
  }

  "Incompatible types" - {
    "1 + '1'" fails

    "1 + /1/" fails
  }

  "Function call" - {
    "file(1)" fails

    "file('/some/file')" hasType Type.String
  }

  "Number binary operations" - {
    "2 + 1" hasType Type.Number
    "2 * 1" hasType Type.Number
    "2 / 1" hasType Type.Number
    "2 - 1" hasType Type.Number
  }

  "String Binary Operations" - {
    "'a' / 'b'" fails

    "'b' - 'b'" fails

    "'b' * 'b'" fails

    "'b' <= 'b'" fails

    "'b' < 'b'" fails

    "'b' >= 'b'" fails

    "'b' > 'b'" fails

    "'a' != 'b'" hasType Type.Bool

    "'a' = 'b'" hasType Type.Bool

    "'b' + 'b'" hasType Type.String
  }

  "Bool binary operations" - {
    "true" hasType Type.Bool

    "false" hasType Type.Bool

    "true = (1 = 2)" hasType Type.Bool

    "(1 = 1) = ('a' = 'b')" hasType Type.Bool

    "true - false" fails

    "true && false" hasType Type.Bool

    "true || false" hasType Type.Bool

    "1 = 1 || 2 = 2" hasType Type.Bool

  }

trait AnalysisDsl extends AnyFreeSpec with Matchers:

  def analyze(e: Indexed[cst.Expression[Indexed]]): Either[TrackedError, ast.Expression] =
    analyzeExpression(e).runA(Map.empty).map(_.fix)
  def analyze(c: String): Either[TrackedError, ast.Expression] =
    analyze(expression.parseAll(c).getOrElse(sys.error("testspec is wrong, cannot parse input")))

  extension (c: cst.Expression[Indexed])

    def analyzesTo(expected: ast.Expression): Unit =
      c.toString in {
        analyze(Indexed(c, Position.empty)) match {
          case Left(error)   => fail(error.toString)
          case Right(result) => assert(result == expected)
        }
      }

  extension (c: String) {

    def analyzesTo(expected: ast.Expression): Unit = {
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
          case Left(error)   => fail(error.format(c))
          case Right(Fix(r)) => assert(r.expressionType == expected)
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
          case Left(e)       => succeed
          case Right(result) => fail("expected error")
        }
      }

  }
