package duff.jagsql
package eval

import duff.jagsql.ast.*
import duff.jagsql.cst.{Expression, Literal}
import duff.jagsql.parser.expression

import scala.language.postfixOps

import cats.*
import cats.data.State
import cats.implicits.*

import org.scalatest
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class evalSpec extends EvalDsl:
  "1" evaluatesTo Value.VNumber(BigDecimal(1))

  "'1'" evaluatesTo Value.VString("1")

  "1 + 1" evaluatesTo Value.VNumber(BigDecimal(2))

  "1 + 1 * 2" evaluatesTo Value.VNumber(BigDecimal(3))

  "2 * 1 + 1" evaluatesTo Value.VNumber(BigDecimal(3))

  "'lol' = 'lol'" evaluatesTo Value.VBoolean(true)

  "array(1,2,3)" evaluatesTo Value.VArray(List(Value.VNumber(1), Value.VNumber(2), Value.VNumber(3)))

trait EvalDsl extends AnyFreeSpec with Matchers:

  private def analyze(e: cst.Expression): Either[String, ast.Expression] = analyzeExpression(e).runA(Map.empty)

  private def analyze(c: String): Either[String, ast.Expression] =
    analyze(expression.parseAll(c).getOrElse(sys.error("testspec is wrong, cannot parse input")))

  def evaluate(c: String): Either[String, Value] =
    for {
      analyzed  <- analyze(c)
      evaluated <- eval(analyzed).asRight
    } yield evaluated(Map.empty)

  extension (c: String) {

    def evaluatesTo(expected: Value): Unit = {
      import ast.*

      c.toString in {
        evaluate(c) match {
          case Left(error)   => fail(error)
          case Right(result) => assert(result == expected)
        }
      }
    }

    def succeeds: Unit =
      c.toString in {
        evaluate(c) match {
          case Left(e)  => fail(e)
          case Right(_) => succeed
        }
      }

    def fails: Unit =
      c.toString in {
        evaluate(c) match {
          case Left(_)  => succeed
          case Right(_) => fail("expected error")
        }
      }

  }
