package duff.jagsql
package ast.expressions

import duff.jagsql.ast.{AggregateFunction, ExpressionF}
import duff.jagsql.ast.ExpressionF.{Binary, FunctionCallExpression}
import duff.jagsql.ast.validation.TrackedError
import duff.jagsql.cst.{Indexed, Position}
import duff.jagsql.parser.expression
import duff.jagsql.std.Fix

import scala.language.postfixOps

import cats.*
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

class ExpressionSpec extends Checkers with AnyFunSpecLike with FunSpecDiscipline {
  import arbitraries.given

  given eqTree[A]: Eq[ast.ExpressionF[A]] = Eq.fromUniversalEquals

  checkAll(
    "ExpressionF Traverse",
    cats.laws.discipline.TraverseTests[ExpressionF].traverse[Int, Int, String, Int, Id, Option]
  )
}

object arbitraries {
  given Arbitrary[ast.Type] = Arbitrary(Gen.oneOf(ast.Type.String, ast.Type.Number, ast.Type.Bool))

  given arbExpressionF[A: Arbitrary]: Arbitrary[ast.ExpressionF[A]] =
    Arbitrary(
      Gen
        .oneOf(
          Gen.const(ast.ExpressionF.LiteralExpression(cst.Literal.StringLiteral("lol"), ast.Type.String)),
          Gen.const(ast.ExpressionF.FieldRef("someid", "someid", ast.Type.String)),
          for {
            a <- Arbitrary.arbitrary[A]
            b <- Arbitrary.arbitrary[A]
          } yield ast.ExpressionF.Binary(a, b, cst.Operator.Plus, ast.Type.String),
          for {
            e <- Arbitrary.arbitrary[A]
          } yield ast.ExpressionF.FunctionCallExpression(ast.max, Seq(e), ast.Type.Number)
        )
    )

}
