package duff.jagsql

import cats.data.NonEmptyList
import cats.parse.Parser
import org.scalactic.anyvals.NonEmptyMap
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

import cst._
import cst.Expression._
import cst.Literal._
import cst.Statement._
import cst.Source._
import SQLParser._

class SQLParserSpec extends SqlParserTestDsl {

  "Literals" - {
    "StringLiteral" - {
      implicit val parser = literal

      "'didier'" parsesTo StringLiteral("didier")
      "''" parsesTo StringLiteral("")
    }

    "NumberLiteral" - {
      implicit val parser = literal

      "10" parsesTo NumberLiteral(BigDecimal(10))
      "0.1" parsesTo NumberLiteral(BigDecimal("0.1"))
      "a" fails
    }

    "RegexLiteral" - {
      implicit val parser = literal

      "/abcd/" parsesTo RegexLiteral("abcd")
      "/(a)/" parsesTo RegexLiteral("(a)")
    }

    "BoolLiteral" - {
      implicit val parser = literal

      "true" parsesTo BoolLiteral(true)
      "false" parsesTo BoolLiteral(false)
    }
  }

  "Expressions" - {
    implicit val parser = expression

    "'salut'" parsesTo (LiteralExpression(StringLiteral("salut")): Expression)
    "1" parsesTo (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression)
    "/didier/" parsesTo (LiteralExpression(RegexLiteral("didier")): Expression)

    "didier(1)" parsesTo (FunctionCallExpression(
      "didier",
      List(LiteralExpression(NumberLiteral(1)))
    ): Expression)

    "file(1)" parses

    "Operator precedence" - {

      "1 + 1 * 2" parses

      "1 * 2 + 1" parses

    }
  }

  val one = LiteralExpression(NumberLiteral(BigDecimal(1)))
  val ttrue = LiteralExpression(BoolLiteral(true))
  val tfalse = LiteralExpression(BoolLiteral(false))

  "BinaryExpression" - {
    implicit val parser = expression
    "'salut'" parsesTo (LiteralExpression(StringLiteral("salut")): Expression)
    "1" parsesTo (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression)

    "1 = 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Equal)))
    "1 != 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Different)))
    "1 <= 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.LessEqual)))
    "1 >= 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.MoreEqual)))
    "1 < 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Less)))
    "1 > 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.More)))
    "1 + 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Plus)))
    "1 - 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Minus)))
    "1 / 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Divided)))
    "1 * 1" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Times)))
    "1 * 1 * 1" parsesTo Binary(
      Binary(one, one, Operator.B(BinaryOperator.Times)),
      one,
      Operator.B(BinaryOperator.Times)
    )
    "true && false" parsesTo (Binary(ttrue, tfalse, Operator.And))

    "true || false" parsesTo (Binary(ttrue, tfalse, Operator.Or))

  }

  "Parens" - {
    implicit val parser = expression

    "('salut')" parsesTo (LiteralExpression(StringLiteral("salut")): Expression)
    "(1)" parsesTo (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression)

    "(1 = 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Equal)))
    "(1 != 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Different)))
    "(1 <= 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.LessEqual)))
    "(1 >= 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.MoreEqual)))
    "(1 < 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Less)))
    "(1 > 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.More)))
    "(1 + 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Plus)))
    "(1 - 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Minus)))
    "(1 / 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Divided)))
    "(1 * 1)" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Times)))
    "(((1 * 1)))" parsesTo (Binary(one, one, Operator.B(BinaryOperator.Times)))

    "1 + 1 + 1" parsesTo Binary(
      Binary(one, one, Operator.B(BinaryOperator.Plus)),
      one,
      Operator.B(BinaryOperator.Plus)
    )

    "1 + (1 * 1)" parsesTo Binary(
      one,
      Binary(one, one, Operator.B(BinaryOperator.Times)),
      Operator.B(BinaryOperator.Plus)
    )

    "(1 * 1) + 1" parses

    "(1 * 1) + (1 * 1)" parsesTo (Binary(
      (Binary(one, one, Operator.B(BinaryOperator.Times))),
      (Binary(one, one, Operator.B(BinaryOperator.Times))),
      Operator.B(BinaryOperator.Plus)
    ))

    "1 + 1 * 1" parsesTo Binary(
      one,
      Binary(one, one, Operator.B(BinaryOperator.Times)),
      Operator.B(BinaryOperator.Plus)
    )

    "1 * 1 + 1" parsesTo Binary(
      Binary(one, one, Operator.B(BinaryOperator.Times)),
      one,
      Operator.B(BinaryOperator.Plus)
    )
  }

  "Statements" - {
    "SELECT" - {
      implicit val parser = selectStatement

      "SELECT" fails

      "SELECT 1" parsesTo SelectStatement(
        NonEmptyList.one(Projection(one)),
        None
      ).asInstanceOf[Statement]

      val expected = SelectStatement(
        NonEmptyList(
          Projection(one),
          Projection(LiteralExpression(StringLiteral("foo"))) ::
            Projection(LiteralExpression(RegexLiteral("bar"))) :: Nil
        ),
        None
      ).asInstanceOf[Statement]

      "SELECT 1, 'foo', /bar/" parsesTo expected
      "SELECT 1,'foo',/bar/" parsesTo expected

      "SELECT 1 FROM foo" parsesTo SelectStatement(
        NonEmptyList.one(Projection(one)),
        Some(
          FromClause(
            NonEmptyList.one(
              FromSource(TableRef("foo"), None)
            )
          )
        )
      )

      "SELECT 1 FROM foo JOIN bar ON 1" parsesTo SelectStatement(
        NonEmptyList.one(Projection(one)),
        Some(
          FromClause(
            NonEmptyList(
              FromSource(TableRef("foo"), None),
              List(
                FromSource(TableRef("bar"), Some(one))
              )
            )
          )
        )
      )

      "SELECT 'a' FROM (SELECT 'b') AS foo" parses

      "SELECT 'a' FROM (SELECT 'b' FROM (SELECT 1) AS bar) AS foo" parses

      "SELECT 1 AS foo" parsesTo SelectStatement(
        NonEmptyList.one(Projection(one, Some("foo")))
      )

      "SELECT 1 AS foo, '2', 'bar' AS baz" parses

      "SELECT foo.bar FROM (SELECT 1 AS bar) AS foo JOIN (SELECT 2 AS bar) AS baz ON foo.bar = baz.bar" parses

      "SELECT col_0 FROM STDIN" parses

    }

    "FROM" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM didier" parsesTo
        SelectStatement(
          NonEmptyList.one(Projection(one)),
          Some(
            FromClause(
              NonEmptyList.one(
                FromSource(TableRef("didier"), None)
              )
            )
          )
        )

      "SELECT 1 FROM didier JOIN tata ON 1" parses

      "SELECT 1 FROM didier JOIN tata ON 1 JOIN toto ON 2 JOIN toto ON 2" parses

      "SELECT 1 FROM didier JOIN tata ON 1 JOIN toto ON 2" parses

      "SELECT 1 FROM didier JOIN tata" fails

      "SELECT 1 FROM didier JOIN tata JOIN toto" fails

      "SELECT 1 FROM didier JOIN tata ON 1 JOIN toto" fails

    }

    "JOIN" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM STDIN JOIN tata ON 2" parses

      "SELECT 1 FROM STDIN JOIN tata ON 3 " parses

      "SELECT 1 FROM STDIN JOIN tata ON 2 JOIN tata ON 2" parses

      "SELECT 1 FROM STDIN JOIN tata ON 2 JOIN tata ON 2 JOIN tata ON 2" parses

      "SELECT 1 FROM STDIN JOIN tata" fails
    }

    "WHERE" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM didier WHERE" fails

      "SELECT 1 FROM didier WHERE 1" parses

      "SELECT 1 FROM didier JOIN tata ON 1 WHERE 2" parses
    }

    "Identifiers" - {
      implicit val parser = selectStatement

      "SELECT foo" parses

      "SELECT foo, bar" parses

      "SELECT foo, 'bar', baz" parses

      "SELECT foo.bar" parses

    }

  }

}

trait SqlParserTestDsl extends AnyFreeSpec with Matchers {

  implicit class TestWrapper[T](l: String) {

    def parsesTo[K >: T](result: T)(implicit p: Parser[K]): Unit =
      s"$l" in {
        p.parseAll(l) match {
          case Left(e)      => fail(e.toString)
          case Right(value) => val _ = assert(value === result)
        }
      }

    def formatError(e: Parser.Error): String = e match {
      case Parser.Error(pos, _) =>
        (1 to pos).map(_ => " ").mkString + "^"
    }

    def parses(implicit p: Parser[_]): Unit =
      s"$l" in {
        p.parseAll(l) match {
          case Left(e)  => fail(l + "\n" + formatError(e) + "\n" + e.toString)
          case Right(_) => val _ = succeed
        }
      }

    def fails(implicit p: Parser[T]): Unit =
      s"$l" in {
        p.parseAll(l) match {
          case Left(_)      => succeed
          case Right(value) => fail(s"Expected failure got $value")
        }
      }

  }

}
