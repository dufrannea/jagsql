package duff.jagsql
package ast

import duff.jagsql.ast.{ComplexType, SimpleType, Type}
import duff.jagsql.ast.SimpleType.Regex
import duff.jagsql.std.RegexWrapper

import cats.data.NonEmptyMap

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class typesSpec extends AnyFreeSpec with Matchers {

  def gen(v: String): List[Type] = {
    val t: NonEmptyMap[String, SimpleType] = NonEmptyMap.of(v -> SimpleType.String)
    val table: ComplexType.Table = ComplexType.Table(t)
    List(Type.Number, SimpleType.Regex(RegexWrapper.build(v).get), Type.String, table)
  }

  "types should be compatible" in {
    // prove (left == right) ==> left.isSubTypeOf(right)
    gen("foo").flatMap(k => gen("bar").map(k -> _)).foreach { case (left, right) =>
      assert((left != right) || (left.isCompatibleWith(right)), s"proven false for $left and $right")
    }

    gen("foo").zip(gen("bar")).foreach { case (l, r) =>
      assert(l.isCompatibleWith(r) && r.isCompatibleWith(l))
    }
  }

}
