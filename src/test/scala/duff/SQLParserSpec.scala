package duff.jagsql

import duff.jagsql.cst.*
import duff.jagsql.cst.Expression.*
import duff.jagsql.cst.Literal.*
import duff.jagsql.cst.Source.*
import duff.jagsql.cst.Statement.*
import duff.jagsql.parser.*

import scala.language.postfixOps

import cats.data.NonEmptyList
import cats.parse.Parser
import cats.syntax.group

import org.scalactic.anyvals.NonEmptyMap
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

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

    "array(1,2,3)" parsesTo (FunctionCallExpression(
      "array",
      Seq(1, 2, 3).map(i => LiteralExpression(NumberLiteral(i)))
    ): Expression)

    "array(1, 2, 3)" parses

    "array( 1 , 2,3 )" parses

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

    "1 = 1" parsesTo (Binary(one, one, BinaryOperator.Equal))
    "1 != 1" parsesTo (Binary(one, one, BinaryOperator.Different))
    "1 <= 1" parsesTo (Binary(one, one, BinaryOperator.LessEqual))
    "1 >= 1" parsesTo (Binary(one, one, BinaryOperator.MoreEqual))
    "1 < 1" parsesTo (Binary(one, one, BinaryOperator.Less))
    "1 > 1" parsesTo (Binary(one, one, BinaryOperator.More))
    "1 + 1" parsesTo (Binary(one, one, BinaryOperator.Plus))
    "1 - 1" parsesTo (Binary(one, one, BinaryOperator.Minus))
    "1 / 1" parsesTo (Binary(one, one, BinaryOperator.Divided))
    "1 * 1" parsesTo (Binary(one, one, BinaryOperator.Times))
    "1 * 1 * 1" parsesTo Binary(
      Binary(one, one, BinaryOperator.Times),
      one,
      BinaryOperator.Times
    )
    "true && false" parsesTo (Binary(ttrue, tfalse, Operator.And))

    "true || false" parsesTo (Binary(ttrue, tfalse, Operator.Or))

  }

  "Parens" - {
    implicit val parser = expression

    "('salut')" parsesTo (LiteralExpression(StringLiteral("salut")): Expression)
    "(1)" parsesTo (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression)

    "(1 = 1)" parsesTo (Binary(one, one, (BinaryOperator.Equal)))
    "(1 != 1)" parsesTo (Binary(one, one, (BinaryOperator.Different)))
    "(1 <= 1)" parsesTo (Binary(one, one, (BinaryOperator.LessEqual)))
    "(1 >= 1)" parsesTo (Binary(one, one, (BinaryOperator.MoreEqual)))
    "(1 < 1)" parsesTo (Binary(one, one, (BinaryOperator.Less)))
    "(1 > 1)" parsesTo (Binary(one, one, (BinaryOperator.More)))
    "(1 + 1)" parsesTo (Binary(one, one, (BinaryOperator.Plus)))
    "(1 - 1)" parsesTo (Binary(one, one, (BinaryOperator.Minus)))
    "(1 / 1)" parsesTo (Binary(one, one, (BinaryOperator.Divided)))
    "(1 * 1)" parsesTo (Binary(one, one, (BinaryOperator.Times)))
    "(((1 * 1)))" parsesTo (Binary(one, one, (BinaryOperator.Times)))

    "1 + 1 + 1" parsesTo Binary(
      Binary(one, one, (BinaryOperator.Plus)),
      one,
      (BinaryOperator.Plus)
    )

    "1 + (1 * 1)" parsesTo Binary(
      one,
      Binary(one, one, (BinaryOperator.Times)),
      (BinaryOperator.Plus)
    )

    "(1 * 1) + 1" parses

    "(1 * 1) + (1 * 1)" parsesTo (Binary(
      (Binary(one, one, BinaryOperator.Times)),
      (Binary(one, one, BinaryOperator.Times)),
      (BinaryOperator.Plus)
    ))

    "1 + 1 * 1" parsesTo Binary(
      one,
      Binary(one, one, (BinaryOperator.Times)),
      (BinaryOperator.Plus)
    )

    "1 * 1 + 1" parsesTo Binary(
      Binary(one, one, (BinaryOperator.Times)),
      one,
      (BinaryOperator.Plus)
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
              FromSource(TableRef("foo", "foo"), None)
            )
          )
        )
      )

      "SELECT 1 FROM foo JOIN bar ON 1" parsesTo SelectStatement(
        NonEmptyList.one(Projection(one)),
        Some(
          FromClause(
            NonEmptyList(
              FromSource(TableRef("foo", "foo"), None),
              List(
                FromSource(TableRef("bar", "bar"), Some(one))
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

      "SELECT col_0 FROM STDIN AS foo" parses

    }

    "FUNCTIONS" - {
      implicit val parser = selectStatement

      "SELECT col_0 FROM SOMEFUNC" fails

      "SELECT col_0 FROM SOMEFUNC() AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1) AS foo" parses

      "SELECT col_0 FROM SOMEFUNC('/foo/bar') AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1, '/foo/bar') AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1, '/foo/bar', /foobar/) AS foo" parses

    }

    "FROM" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM foo" parsesTo
        SelectStatement(
          NonEmptyList.one(Projection(one)),
          Some(
            FromClause(
              NonEmptyList.one(
                FromSource(TableRef("foo", "foo"), None)
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

    "GROUP BY" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM foo GROUP BY" fails

      "SELECT 1 FROM foo GROUP BY 1" parses

      "SELECT 1 AS bar FROM foo GROUP BY bar" parses

      "SELECT 1 AS bar FROM (SELECT 1 AS lol) AS foo GROUP BY bar + 1, bar, bar - 2" parses

      // |  ,CAST(1 AS STRING) AS two
      """|SELECT
         |  1 AS one
         |  , 1 * 42 AS three
         |  ,a.b + 1 AS four
         |  ,a.d -1 AS five
         |  ,a.d -1 + 1 AS six
         |  ,max(a.c - 1) + 1 AS seven
         |FROM
         |  (SELECT 1 AS b, 1 AS d, 1 AS c) AS a
         |GROUP BY a.b, a.d - 1""".stripMargin parses
    }

    "Identifiers" - {
      implicit val parser = selectStatement

      "SELECT foo" parses

      "SELECT foo, bar" parses

      "SELECT foo, 'bar', baz" parses

      "SELECT foo.bar" parses

    }

  }

  "maybeAliased should be able to backtrack" - {
    implicit val parser = maybeAliased(Parser.char('a')) ~ (Parser.char(' ') *> Parser.char('b'))

    "a b" parsesTo (((), None), ())

    "a AS foo b" parses

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
