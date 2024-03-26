package duff.jagsql
package ast
package validation

import duff.jagsql.cst.{Expression, Literal, Position}
import duff.jagsql.eval.Value
import duff.jagsql.parser.Indexed
import duff.jagsql.parser.expression
import duff.jagsql.std.*
import duff.jagsql.std.RegexWrapper

import scala.language.postfixOps

import cats.data.{NonEmptyMap, State}

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

    "regex(/a/, 'a')" hasType SimpleType.Array(Type.String)
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

  "Regex" - {
    "/a/" hasType SimpleType.Regex(RegexWrapper.build("a").get)
  }

  "Subscription" - {

    "struct('foo', 'bar').baz" failsWith (
      """ERROR: Cannot access member baz of type {foo -> String}
        |
        |struct('foo', 'bar').baz
        |^^^^^^^^^^^^^^^^^^^^""".stripMargin
    )

    "struct('foo', 1).foo" hasType Type.Number

    "struct('foo', 1, 'bar', 'baz').bar" hasType Type.String

    "struct('foo', struct('bar', 'baz')).foo" hasType SimpleType.Struct(NonEmptyMap.of("bar" -> Type.String))

    "struct('foo', struct('bar', 'baz')).foo.bar" hasType Type.String

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

    // noinspection UnitMethodIsParameterless
    def hasType(expected: ast.Type): Unit =
      c in {
        analyze(c) match {
          case Left(error)   => fail(error.format(c))
          case Right(Fix(r)) => assert(r.expressionType == expected)
        }
      }

    // noinspection UnitMethodIsParameterless
    def succeeds: Unit =
      c in {
        analyze(c) match {
          case Left(e)       => fail(e.format(c))
          case Right(result) => succeed
        }
      }

    // noinspection UnitMethodIsParameterless
    def fails: Unit =
      c in {
        analyze(c) match {
          case Left(e)       => succeed
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
