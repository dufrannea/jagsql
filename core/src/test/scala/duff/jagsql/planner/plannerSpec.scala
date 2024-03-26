package duff.jagsql
package planner

import duff.jagsql.ast.{AggregateFunction, ComplexType, ExpressionF}
import duff.jagsql.ast.ExpressionF.{Binary, FunctionCallExpression}
import duff.jagsql.ast.validation.TrackedError
import duff.jagsql.cst.Position
import duff.jagsql.parser.{expression, Indexed}
import duff.jagsql.std.Fix

import scala.language.postfixOps

import cats.*
import cats.data.NonEmptyList
import cats.implicits.*
import cats.laws.discipline.FunctorTests

import ast.validation.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.*
import planner.*

class plannerSpec extends AnalysisDsl:

  import cats.implicits.*

  import ast.ExpressionF.*

  def getExpression(s: String, context: Map[String, ComplexType.Table] = Map.empty) =
    analyze(s)(
      using context
    ) match {
      case Left(error)  => fail(new Throwable(error.format(s)))
      case Right(value) => value
    }

  "replace expressions" in {
    val groupByExpressions = Map(
      getExpression("max(1)") -> "col_0",
      getExpression("max(2)") -> "col_1"
    )

    val e = getExpression("1 + max(1) + max(2)")
    val actualExpression = r3(groupByExpressions, "foo")(e)

    val expectedExpression = {
      val c: ComplexType.Table =
        ComplexType.Table(NonEmptyList.of("col_0" -> ast.Type.Number, "col_1" -> ast.Type.Number).toNem)

      getExpression("1 + foo.col_0 + foo.col_1", Map("foo" -> c))
    }
    assert(actualExpression == expectedExpression)

  }

  "extract aggregates" in {
    val e = getExpression("1 + max(1) + max(2)")
    val z = getAggregateFunctions(e)
    assert(z.size == 2)
  }

  "extract only toplevel aggregate when nested" in {
    val e = getExpression("1 + max(max(1) + max(2))")
    val z = getAggregateFunctions(e)
    assert(z.size == 1)
  }

  "extract no aggregate" in {
    val e = getExpression("1 + 10")
    val z = getAggregateFunctions(e)
    assert(z.isEmpty)
  }

trait AnalysisDsl extends AnyFreeSpec with Matchers:

  def analyze(
    e: Indexed[cst.Expression[Indexed]]
  )(
    using context: Map[String, ComplexType.Table]
  ): Either[TrackedError, ast.Expression] =
    analyzeExpression(e).runA(context).map(_.fix)

  def analyze(
    c: String
  )(
    using context: Map[String, ComplexType.Table] = Map.empty[String, ComplexType.Table]
  ): Either[TrackedError, ast.Expression] =
    analyze(expression.parseAll(c).getOrElse(sys.error("testspec is wrong, cannot parse input")))(
      using context
    )

  extension (c: cst.Expression[Indexed])

    def analyzesTo(expected: ast.Expression): Unit =
      c.toString in {
        analyze(Indexed(c, Position.empty))(
          using Map.empty
        ) match {
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
