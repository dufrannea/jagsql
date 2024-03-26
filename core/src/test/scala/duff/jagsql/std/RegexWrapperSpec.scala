package duff.jagsql
package std

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class RegexWrapperSpec extends AnyFreeSpec with Matchers {

  "group counts" - {

    "Simple" - {

      "" ==> 0

      "()" ==> 1

      "a()" ==> 1

      "()a" ==> 1

      "a()a" ==> 1

      "((()))" ==> 3
    }

    "Square brackets" - {

      "[()]" ==> 0

      "([()])" ==> 1
    }

    "Escapings" - {

      "()\\(\\)" ==> 1

      "(\\(()\\))" ==> 2

      "\\[()\\]" ==> 1

    }

    "Non capturing" - {

      "(?:)" ==> 0
    }

    "Named groups" - {

      "Simple" - {

        "" ==> 0

        "(?<foo>)" ==> (0 -> Some("foo"))

        "(?<foo>)(?<bar>)" ==> (0 -> Some("foo")) :: (1 -> Some("bar")) :: Nil

        "a(?<foo>)" ==> (0 -> Some("foo"))

        "(?<foo>)a" ==> (0 -> Some("foo"))

        "a(?<foo>)a" ==> (0 -> Some("foo"))

        "(?<foo>(?<bar>(?<baz>)))" ==> (0 -> Some("foo")) :: (1 -> Some("bar")) :: (2 -> Some("baz")) :: Nil

      }

      "Square brackets" - {

        "[(?<foo>)]" ==> Nil

        "(?<foo>[(?<bar>)])" ==> (0 -> Some("foo"))
      }

      "Escapings" - {

        "(?<foo>)\\(?<bar>\\)" ==> (0 -> Some("foo"))

        "(?<foo>\\(?<bar>(?<baz>)\\))" ==> (0 -> Some("foo")) :: (1 -> Some("baz")) :: Nil

        "\\[(?<foo>)\\]" ==> (0 -> Some("foo"))

      }

    }

  }

  extension (s: String) {

    def ==>(r: Int) =
      s in {
        assert(RegexWrapper.build(s).get.groupCount == r)

      }

    def ==>(arg: (Int, Option[String])) =
      s in {
        assert(RegexWrapper.build(s).get.groups == arg :: Nil)

      }

    def ==>(args: List[(Int, Option[String])]) =
      s in {
        assert(RegexWrapper.build(s).get.groups == args)

      }

  }

}
