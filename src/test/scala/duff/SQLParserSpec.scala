package duff

import cats.data.NonEmptyList
import cats.parse.Parser
import duff.cst._
import duff.cst.Expression._
import duff.cst.Literal._
import duff.cst.Statement._
import duff.cst.Source._
import duff.SQLParser._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class SQLParserSpec extends AnyFreeSpec with Matchers {

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

  "Literals" - {
    "StringLiteral" - {
      implicit val parser = stringLiteral

      "'didier'" parsesTo StringLiteral("didier")
      "''" parsesTo StringLiteral("")
    }

    "NumberLiteral" - {
      implicit val parser = numberLiteral

      "10" parsesTo NumberLiteral(BigDecimal(10))
      "0.1" parsesTo NumberLiteral(BigDecimal("0.1"))
      "a" fails
    }

    "RegexLiteral" - {
      implicit val parser = regexLiteral

      "/abcd/" parsesTo RegexLiteral("abcd")
      "/(a)/" parsesTo RegexLiteral("(a)")
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
  }

  val one = LiteralExpression(NumberLiteral(BigDecimal(1)))

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
      one,
      Binary(one, one, Operator.B(BinaryOperator.Times)),
      Operator.B(BinaryOperator.Times)
    )
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
      one,
      Binary(one, one, Operator.B(BinaryOperator.Plus)),
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

  }

  "Statements" - {
    "SELECT" - {
      implicit val parser = selectStatement

      "SELECT" fails

      "SELECT 1" parsesTo SelectStatement(
        NonEmptyList.one(one),
        None
      ).asInstanceOf[Statement]

      val expected = SelectStatement(
        NonEmptyList(
          one,
          LiteralExpression(StringLiteral("foo")) ::
            LiteralExpression(RegexLiteral("bar")) :: Nil
        ),
        None
      ).asInstanceOf[Statement]

      "SELECT 1, 'foo', /bar/" parsesTo expected
      "SELECT 1,'foo',/bar/" parsesTo expected

      "SELECT 1 FROM didier" parsesTo SelectStatement(
        NonEmptyList.one(one),
        Some(
          FromClause(
            NonEmptyList.one(
              FromItem(TableRef("didier"), None)
            )
          )
        )
      )

      "SELECT 1 FROM didier JOIN tata ON 1" parsesTo SelectStatement(
        NonEmptyList.one(one),
        Some(
          FromClause(
            NonEmptyList(
              FromItem(TableRef("didier"), None),
              List(
                FromItem(TableRef("tata"), Some(one))
              )
            )
          )
        )
      )
    }

    "FROM" - {
      implicit val parser = fromClause

      "FROM didier" parsesTo
        FromClause(
          NonEmptyList.one(
            FromItem(TableRef("didier"), None)
          )
        )

      "FROM didier JOIN tata ON 1" parses

      "FROM didier JOIN tata ON 1 JOIN toto ON 2 JOIN toto ON 2" parses

      "FROM didier JOIN tata ON 1 JOIN toto ON 2" parses

      "FROM didier JOIN tata" fails

      "FROM didier JOIN tata JOIN toto" fails

      "FROM didier JOIN tata ON 1 JOIN toto" fails

    }

    "JOIN" - {
      implicit val parser = joinClausea

      "JOIN tata ON 2" parses

      "JOIN tata ON 3 " parses

      "JOIN tata ON 2 JOIN tata ON 2" parses

      "JOIN tata ON 2 JOIN tata ON 2 JOIN tata ON 2" parses

      "JOIN tata" fails
    }

    "WHERE" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM didier WHERE" fails

      "SELECT 1 FROM didier WHERE 1" parses

      "SELECT 1 FROM didier JOIN tata ON 1 WHERE 2" parses
    }
  }
}
