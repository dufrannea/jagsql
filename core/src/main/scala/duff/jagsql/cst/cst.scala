package duff.jagsql
package cst

import duff.jagsql.cst.Literal.{NumberLiteral, RegexLiteral, StringLiteral}
import duff.jagsql.cst.Projection
import duff.jagsql.std.FunctorK
import duff.jagsql.std.FunctorK.{given, *}
import duff.jagsql.std.RegexWrapper

import scala.math.BigDecimal
import scala.util.matching.Regex

import cats.{~>, data, Functor}
import cats.CoflatMap
import cats.data.NonEmptyList

case class Position(from: Int, to: Int) {
  def join(o: Position) = Position(Math.min(from, o.from), Math.max(to, o.to))
}

object Position {
  def empty = Position(0, 0)
}

enum Literal {
  case StringLiteral(a: String)
  case NumberLiteral(a: BigDecimal)
  case RegexLiteral(str: RegexWrapper)
  case BoolLiteral(value: Boolean)
}

object Literal {
  def string[F[_]](s: String): StringLiteral = StringLiteral(s)

  def int[F[_]](s: BigDecimal): NumberLiteral = NumberLiteral(s)

  def regex[F[_]](r: RegexWrapper): RegexLiteral = RegexLiteral(r)
}

enum BinaryOperator extends Operator {
  case Equal, Different, Less, More, LessEqual, MoreEqual, Plus, Minus, Times, Divided, Or, And
}

enum UnaryOperator extends Operator {
  case Not
}

sealed trait Operator

object Operator {
  val Equal = BinaryOperator.Equal
  val Different = BinaryOperator.Different
  val Less = BinaryOperator.Less
  val More = BinaryOperator.More
  val LessEqual = BinaryOperator.LessEqual
  val MoreEqual = BinaryOperator.MoreEqual
  val Plus = BinaryOperator.Plus
  val Minus = BinaryOperator.Minus
  val Times = BinaryOperator.Times
  val Divided = BinaryOperator.Divided
  val Or = BinaryOperator.Or
  val And = BinaryOperator.And
  val Not = UnaryOperator.Not
}

case class Projection[F[_]](expression: F[Expression[F]], maybeAlias: Option[F[String]] = None)

object Projection {

  given FunctorK[Projection] = new FunctorK[Projection] {
    override def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: Projection[F]): Projection[G] =
      Projection(s.expression.emap(g), s.maybeAlias.map(p => g(p)))
  }

}

enum Expression[F[_]] {
  case LiteralExpression(literal: F[Literal])
  case FunctionCallExpression(name: F[String], arguments: Seq[F[Expression[F]]])
  case Binary(left: F[Expression[F]], right: F[Expression[F]], operator: F[Operator])
  case Unary(expression: F[Expression[F]], operator: F[Operator])
  case IdentifierExpression(identifier: F[String])
  case StructAccess(inner: F[Expression[F]], accessors: F[NonEmptyList[String]])
}

object Expression {

  given FunctorK[Expression] = new FunctorK[Expression] {
    import duff.jagsql.cst.Expression.*

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: Expression[F]): Expression[G] =
      s match {
        case l @ LiteralExpression(literal)          => LiteralExpression(g(literal))
        case FunctionCallExpression(name, arguments) =>
          FunctionCallExpression(g(name), arguments.nestedMap(g))
        case Binary(left, right, operator)           =>
          Binary(left.emap(g), right.emap(g), g(operator))
        case Unary(expression, operator)             =>
          Unary(expression.emap(g), g(operator))
        case IdentifierExpression(identifier)        => IdentifierExpression(g(identifier))
        case StructAccess(expression, accessors)     => StructAccess(expression.emap(g), g(accessors))
      }

  }

}

case class Aliased[T](source: T, alias: Option[String])

enum Source[F[_]] {
  case StdIn(alias: F[String])
  case Dual(alias: F[String])
  case TableRef(identifier: F[String], alias: F[String])
  case SubQuery(statement: F[Statement.SelectStatement[F]], alias: F[String])
  case TableFunction(name: F[String], arguments: List[F[Literal]], alias: F[String])
}

object Source {

  given FunctorK[Source] = new FunctorK[Source] {
    import Source.*

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: Source[F]): Source[G] =
      s match {
        case in: StdIn[F]          => StdIn(g(in.alias))
        case in: Dual[F]           => Dual(g(in.alias))
        case ref: TableRef[F]      => TableRef(g(ref.identifier), g(ref.alias))
        case subQuery: SubQuery[F] =>
          SubQuery(subQuery.statement.emap(g), g(subQuery.alias))
        case tf: TableFunction[F]  => TableFunction[G](g(tf.name), tf.arguments.map(arg => g(arg)), g(tf.alias))
      }

  }

}

case class FromSource[F[_]](source: F[Source[F]], maybeJoinPredicates: Option[F[Expression[F]]])

object FromSource {

  given FunctorK[FromSource] = new FunctorK[FromSource] {

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: FromSource[F]): FromSource[G] =
      FromSource(
        s.source.emap(g),
        s.maybeJoinPredicates.nestedMap(g)
      )

  }

}

case class FromClause[F[_]](items: NonEmptyList[F[FromSource[F]]])

object FromClause {

  given FunctorK[FromClause] = new FunctorK[FromClause] {

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: FromClause[F]): FromClause[G] =
      FromClause(s.items.nestedMap(g))
  }

}

case class WhereClause[F[_]](expression: F[Expression[F]])

object WhereClause {

  given FunctorK[WhereClause] = new FunctorK[WhereClause] {

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: WhereClause[F]): WhereClause[G] =
      WhereClause(s.expression.emap(g))
  }

}

case class GroupByClause[F[_]](expressions: NonEmptyList[F[Expression[F]]])

object GroupByClause {

  given FunctorK[GroupByClause] = new FunctorK[GroupByClause] {

    def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: GroupByClause[F]): GroupByClause[G] =
      GroupByClause(s.expressions.nestedMap(g))
  }

}

sealed trait Statement[F[_]]

object Statement {

  import cats.*
  import cats.~>
  import cats.implicits.*

  case class SelectStatement[F[_]](
    projections: NonEmptyList[F[Projection[F]]],
    fromClause: Option[F[FromClause[F]]] = None,
    whereClause: Option[F[WhereClause[F]]] = None,
    groupByClause: Option[F[GroupByClause[F]]] = None
  ) extends Statement[F]

  object SelectStatement {

    implicit val ffunctorStatement: FunctorK[SelectStatement] = new FunctorK[SelectStatement] {

      def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: SelectStatement[F]): SelectStatement[G] =
        s match {
          case SelectStatement(projections, fromClause, whereClause, groupByClause) =>
            SelectStatement[G](
              projections.nestedMap(g),
              fromClause.nestedMap(g),
              whereClause.nestedMap(g),
              groupByClause.nestedMap(g)
            )
        }

    }

  }

}
