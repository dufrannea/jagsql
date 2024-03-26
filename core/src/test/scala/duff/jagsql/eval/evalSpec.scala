package duff.jagsql
package eval

import duff.jagsql.ast.*
import duff.jagsql.ast.validation.*
import duff.jagsql.cst.{Expression, Literal}
import duff.jagsql.parser.{expression, Indexed}
import duff.jagsql.std.RegexWrapper

import scala.language.postfixOps

import cats.*
import cats.data.{NonEmptyMap, State}
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

  "struct('1',2)" evaluatesTo Value.VStruct(Map(("1" -> Value.VNumber(2))))

  "struct('a',1).a" evaluatesTo Value.VNumber(1)

  "struct('a',1, 'b', 'foo').b" evaluatesTo Value.VString("foo")

  "struct('a','foo').a + struct('b', 'bar').b" evaluatesTo Value.VString("foobar")

  "struct('1',2, '3', '4')" evaluatesTo Value.VStruct(
    Map(
      "1" -> Value.VNumber(2),
      "3" -> Value.VString("4")
    )
  )

  "-1" evaluatesTo Value.VNumber(BigDecimal(-1))

  "!true" evaluatesTo Value.VBoolean(false)

  "!!true" evaluatesTo Value.VBoolean(true)

  "/a/" evaluatesTo Value.VRegex(RegexWrapper.build("a").get)

  "regex(/a/, 'a')" evaluatesTo Value.VArray(Value.VString("a") :: Nil)

  "regex(/(a)(b)/, 'zab')" evaluatesTo Value.VArray(Nil)

  "regex(/(a)(b)/, 'ab')" evaluatesTo Value.VArray(Value.VString("a") :: Value.VString("b") :: Nil)

  "regex_struct(/(a)/, 'a')" evaluatesTo Value.VStruct(Map("group_0" -> Value.VString("a")))

  "regex_struct(/(a)(b)/, 'ab')" evaluatesTo Value.VStruct(
    Map(
      "group_0" -> Value.VString("a"),
      "group_1" -> Value.VString("b")
    )
  )

  "regex_struct(/(?<foo>a)(?<bar>b)/, 'ab')" evaluatesTo Value.VStruct(
    Map(
      "foo" -> Value.VString("a"),
      "bar" -> Value.VString("b")
    )
  )

trait EvalDsl extends AnyFreeSpec with Matchers:

  private def analyze(e: Indexed[cst.Expression[Indexed]]): Either[TrackedError, ast.Expression] =
    analyzeExpression(e).runA(Map.empty).map(_.fix)

  private def analyze(c: String): Either[TrackedError, ast.Expression] =
    analyze(expression.parseAll(c) match {
      case scala.util.Left(error) => sys.error(s"testspec is wrong, cannot parse input $error")
      case scala.util.Right(a)    => a
    })

  def evaluate(c: String): Either[TrackedError, Value] =
    for {
      analyzed  <- analyze(c)
      evaluated <- eval(analyzed).asRight
    } yield evaluated(Map.empty)

  extension (c: String) {

    def evaluatesTo(expected: Value): Unit = {
      import ast.*

      c in {
        evaluate(c) match {
          case Left(error)   => fail(error.toString)
          case Right(result) => assert(result == expected)
        }
      }
    }

    def succeeds(): Unit =
      c in {
        evaluate(c) match {
          case Left(e)  => fail(e.toString)
          case Right(_) => succeed
        }
      }

    def fails(): Unit =
      c in {
        evaluate(c) match {
          case Left(_)  => succeed
          case Right(_) => fail("expected error")
        }
      }

  }
