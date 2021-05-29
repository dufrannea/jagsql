package duff

import cats.parse.Parser
import duff.AST._
import duff.SQLParser._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.language.postfixOps

class SQLParserSpec extends AnyFreeSpec with Matchers {

  implicit class TestWrapper[T](l: String) {

    def ==>[K >: T](result: T)(implicit p: Parser[K]): Unit =
      s"$l" in {
        p.parse(l) match {
          case Left(e)           => fail(e.toString)
          case Right((_, value)) => val _ = assert(value === result)
        }
      }

    def !!(implicit p: Parser[T]): Unit =
      s"$l" in {
        p.parse(l) match {
          case Left(_)           => succeed
          case Right((_, value)) => fail(s"Expected failure got $value")
        }
      }
  }

  "Literals" - {
    "StringLiteral" - {
      implicit val parser = stringLiteral

      "'didier'" ==> StringLiteral("didier")
      "''" ==> StringLiteral("")
    }

    "NumberLiteral" - {
      implicit val parser = numberLiteral

      "10" ==> NumberLiteral(BigDecimal(10))
      "0.1" ==> NumberLiteral(BigDecimal("0.1"))
      "a" !!
    }

    "RegexLiteral" - {
      implicit val parser = regexLiteral

      "/abcd/" ==> RegexLiteral("abcd")
      "/(a)/" ==> RegexLiteral("(a)")
    }
  }

  "Expressions" - {
    implicit val parser = expression

    "'salut'" ==> (LiteralExpression(StringLiteral("salut")): Expression)
    "1" ==> (LiteralExpression(NumberLiteral(BigDecimal("1"))): Expression)
    "/didier/" ==> (LiteralExpression(RegexLiteral("didier")): Expression)

//    "SELECT 'salut'" ==>
  }
//  "Statements" - {
//    "SELECT" - {
//      implicit val parser = selectStatement
//      "SELECT 1" ==> ???
//    }
//  }
}
