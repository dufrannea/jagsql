package duff.jagsql
package parser

import duff.jagsql.cst.*
import duff.jagsql.cst.Expression.*
import duff.jagsql.cst.Literal.*
import duff.jagsql.cst.Source.*
import duff.jagsql.cst.Statement
import duff.jagsql.cst.Statement.SelectStatement
import duff.jagsql.parser.*
import duff.jagsql.std.FunctorK
import duff.jagsql.std.RegexWrapper

import scala.language.postfixOps
import scala.util.matching.Regex

import cats.*
import cats.data.NonEmptyList
import cats.implicits.*
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

      "/abcd/" parsesTo RegexLiteral(RegexWrapper.build("abcd").get)
      "/(a)/" parsesTo RegexLiteral(RegexWrapper.build("(a)").get)
    }

    "BoolLiteral" - {
      implicit val parser = literal

      "true" parsesTo BoolLiteral(true)
      "false" parsesTo BoolLiteral(false)
    }
  }

  "Expressions" - {
    implicit val parser = expression
    import duff.jagsql.cst.Statement.*

    "'salut'".parsesToM(LiteralExpression[Id](StringLiteral("salut")))

    "1" parsesToM (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression[Id])
    "/didier/" parsesToM (LiteralExpression(RegexLiteral(RegexWrapper.build("didier").get)): Expression[Id])

    "didier(1)" parsesToM (FunctionCallExpression(
      "didier",
      List(LiteralExpression(NumberLiteral(1)))
    ): Expression[Id])

    "file(1)" parses

    "array(1,2,3)" parsesToM (FunctionCallExpression(
      "array",
      Seq(1, 2, 3).map(i => LiteralExpression(NumberLiteral(i)))
    ): Expression[Id])

    "array(1, 2, 3)" parses

    "array( 1 , 2,3 )" parses

    "Operator precedence" - {

      "1 + 1 * 2" parses

      "1 * 2 + 1" parses

    }
  }

  val one: Expression[Id] = LiteralExpression(NumberLiteral(BigDecimal(1)))
  val ttrue = LiteralExpression[Id](BoolLiteral(true))
  val tfalse = LiteralExpression[Id](BoolLiteral(false))

  "BinaryExpression" - {
    implicit val parser = expression
    "'salut'" parsesToM (LiteralExpression(StringLiteral("salut")): Expression[Id])
    "1" parsesToM (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression[Id])

    "1 = 1" parsesToM (Binary(one, one, BinaryOperator.Equal): Expression[Id])
    "1 != 1" parsesToM (Binary(one, one, BinaryOperator.Different): Expression[Id])
    "1 <= 1" parsesToM (Binary(one, one, BinaryOperator.LessEqual): Expression[Id])
    "1 >= 1" parsesToM (Binary(one, one, BinaryOperator.MoreEqual): Expression[Id])
    "1 < 1" parsesToM (Binary(one, one, BinaryOperator.Less): Expression[Id])
    "1 > 1" parsesToM (Binary(one, one, BinaryOperator.More): Expression[Id])
    "1 + 1" parsesToM (Binary(one, one, BinaryOperator.Plus): Expression[Id])
    "1 - 1" parsesToM (Binary(one, one, BinaryOperator.Minus): Expression[Id])
    "1 / 1" parsesToM (Binary(one, one, BinaryOperator.Divided): Expression[Id])
    "1 * 1" parsesToM (Binary(one, one, BinaryOperator.Times): Expression[Id])
    "1 * 1 * 1" parsesToM (Binary(
      Binary(one, one, BinaryOperator.Times),
      one,
      BinaryOperator.Times
    ): Expression[Id])
    "true && false" parsesToM (Binary(ttrue, tfalse, Operator.And): Expression[Id])

    "true || false" parsesToM (Binary(ttrue, tfalse, Operator.Or): Expression[Id])

  }

  "UnaryExpression" - {
    implicit val parser = expression
    "!true" parses

    "!!true" parses

    "!(true && false)" parses

  }

  "Subscription" - {
    implicit val parser = expression

    "struct('a', 'b').a" parses

    "(1).a" parses

    "somefunc('a').a" parses

  }

  "Parens" - {
    implicit val parser = expression

    "('salut')" parsesToM (LiteralExpression(StringLiteral("salut")): Expression[Id])
    "(1)" parsesToM (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression[Id])

    "(1 = 1)" parsesToM (Binary(one, one, (BinaryOperator.Equal)): Expression[Id])
    "(1 != 1)" parsesToM (Binary(one, one, (BinaryOperator.Different)): Expression[Id])
    "(1 <= 1)" parsesToM (Binary(one, one, (BinaryOperator.LessEqual)): Expression[Id])
    "(1 >= 1)" parsesToM (Binary(one, one, (BinaryOperator.MoreEqual)): Expression[Id])
    "(1 < 1)" parsesToM (Binary(one, one, (BinaryOperator.Less)): Expression[Id])
    "(1 > 1)" parsesToM (Binary(one, one, (BinaryOperator.More)): Expression[Id])
    "(1 + 1)" parsesToM (Binary(one, one, (BinaryOperator.Plus)): Expression[Id])
    "(1 - 1)" parsesToM (Binary(one, one, (BinaryOperator.Minus)): Expression[Id])
    "(1 / 1)" parsesToM (Binary(one, one, (BinaryOperator.Divided)): Expression[Id])
    "(1 * 1)" parsesToM (Binary(one, one, (BinaryOperator.Times)): Expression[Id])
    "(((1 * 1)))" parsesToM (Binary(one, one, (BinaryOperator.Times)): Expression[Id])

    "1 + 1 + 1" parsesToM (Binary(
      Binary(one, one, (BinaryOperator.Plus)),
      one,
      (BinaryOperator.Plus)
    ): Expression[Id])

    "1 + (1 * 1)" parsesToM (Binary(
      one,
      Binary(one, one, (BinaryOperator.Times)),
      (BinaryOperator.Plus)
    ): Expression[Id])

    "(1 * 1) + 1" parses

    "(1 * 1) + (1 * 1)" parsesToM (Binary(
      (Binary(one, one, BinaryOperator.Times)),
      (Binary(one, one, BinaryOperator.Times)),
      (BinaryOperator.Plus)
    ): Expression[Id])

    "1 + 1 * 1" parsesToM (Binary(
      one,
      Binary(one, one, (BinaryOperator.Times)),
      (BinaryOperator.Plus)
    ): Expression[Id])

    "1 * 1 + 1" parsesToM (Binary(
      Binary(one, one, (BinaryOperator.Times)),
      one,
      (BinaryOperator.Plus)
    ): Expression[Id])
  }

  "Statements" - {
    "SELECT" - {
      implicit val parser = selectStatement

      "SELECT" fails

      "SELECT 1" parsesToM Statement.SelectStatement[Id](
        NonEmptyList.one(Projection(one)),
        None
      )

      val expected = Statement.SelectStatement[Id](
        NonEmptyList(
          Projection[Id](one),
          Projection[Id](LiteralExpression[Id](StringLiteral("foo"))) ::
            Projection[Id](LiteralExpression[Id](RegexLiteral(RegexWrapper.build("bar").get))) :: Nil
        ),
        None
      )

      "SELECT 1, 'foo', /bar/" parsesToM expected
      "SELECT 1,'foo',/bar/" parsesToM expected

      "SELECT 1 FROM foo" parsesToM (SelectStatement[Id](
        NonEmptyList.one(Projection(one)),
        Some(
          FromClause(
            NonEmptyList.one(
              FromSource(TableRef("foo", "foo"), None)
            )
          )
        )
      ))

      "SELECT 1 FROM foo JOIN bar ON 1" parsesToM SelectStatement[Id](
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

      "SELECT 1 AS foo" parsesToM SelectStatement[Id](
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

      "SELECT col_0 FROM SOMEFUNC('lol') AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1) AS foo" parses

      "SELECT col_0 FROM SOMEFUNC('/foo/bar') AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1, '/foo/bar') AS foo" parses

      "SELECT col_0 FROM SOMEFUNC(1, '/foo/bar', /foobar/) AS foo" parses

    }

    "FROM" - {
      implicit val parser = selectStatement

      "SELECT 1 FROM foo" parsesToM
        SelectStatement[Id](
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

    "Comments" - {
      implicit val parser = selectStatement

      """|SELECT --comment
         |--comment
         | --comment
         |  1 --comment
         |AS one --comment
         |  , 1 * 42 AS three
         |  ,a.b + 1 AS four
         |  ,a.d -1 AS five
         |  ,a.d -1 + 1 AS six
         |  ,max(a.c - 1) + 1 AS seven
         |FROM --comment
         |  (SELECT
         |   --comment
         |   1 AS b, 1 AS d, 1 AS c) AS a
         |GROUP BY  --comment
         |a.b, a.d - 1""".stripMargin parses

    }

    "Identifiers" - {
      implicit val parser = selectStatement

      "SELECT foo" parses

      "SELECT foo, bar" parses

      "SELECT foo, 'bar', baz" parses

      "SELECT foo.bar" parses

    }

  }

  "Comments" - {
    implicit val parser = expression

    "lol -- foo" parses

    """1 --foo
      | --bar 
      |+
      |1""".stripMargin.parses
  }
  // "maybeAliased should be able to backtrack" - {
  //  implicit val parser: Parser[(Indexed[(Indexed[Nothing], Option[Indexed[String]])], Unit)] = maybeAliased(Parser.char('a')) ~ (Parser.char(' ') *> Parser.char('b'))

  //  "a b" parsesTo (((), None), ())

  //  "a AS foo b" parses

  // }

}

trait SqlParserTestDsl extends AnyFreeSpec with Matchers {

  import cats.~>

  val unannotate = new ~>[Indexed, Id] {
    def apply[A](fa: Indexed[A]): Id[A] = fa.value
  }

  implicit class TestWrapper[T](l: String) {
    import duff.jagsql.std.FunctorK.*

    def parsesTo[K >: T](result: T)(implicit p: Parser[Indexed[K]]): Unit =
      s"$l" in {
        p.parseAll(l) match {
          case Left(e)      => fail(e.toString)
          case Right(value) => val _ = assert(value.value === result)
        }
      }

    // Parser[Indexed[Statement[Indexed]]]
    // Parser[F[M[F]]
    def parsesToM[M[_[_]]: FunctorK](result: M[Id])(implicit p: Parser[Indexed[M[Indexed]]]): Unit =
      s"$l" in {
        p.parseAll(l) match {
          case Left(e)      => fail(e.toString)
          case Right(value) =>
            val cleanedValue: M[Id] = value.value.mapK(unannotate)
            val _ = assert(cleanedValue === result)
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
