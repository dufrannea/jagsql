package duff
package ast

import cats._
import cats.implicits._
import cats.data.NonEmptyList
import cats.data.NonEmptyMap

import scala.language.experimental
import scala.util.Try
import duff.cst.Operator
import duff.ast
import cats.data.State

enum SimpleType {
  case Number
  case String
  case Bool

}

enum ComplexType {
  case Table(cols: NonEmptyMap[String, SimpleType])
}

enum Type {
  case Simple(st: SimpleType)
  case Complex(st: ComplexType)
  case Any
}

object Type {
  val Number = Type.Simple(SimpleType.Number)
  val String = Type.Simple(SimpleType.String)
  val Bool = Type.Simple(SimpleType.Bool)
  def Table(cols: NonEmptyMap[String, SimpleType]) = Type.Complex(ComplexType.Table(cols))
}

import Type._

enum Function(val args: List[Type], val returnType: Type) {
  case file extends Function(Type.String :: Nil, Type.String)
}

enum ExpressionF[K] {

  def expressionType: Type

  case LiteralExpression(literal: cst.Literal, expressionType: Type)
  case FunctionCallExpression(name: String, arguments: Seq[K], expressionType: Type)
  case Binary(left: K, right: K, operator: cst.Operator, expressionType: Type)
  case Unary(expression: K, expressionType: Type)
  case FieldRef(value: String, expressionType: Type)
}

object ExpressionF {

  given Functor[ExpressionF] with

    def map[A, B](fa: ExpressionF[A])(f: A => B): ExpressionF[B] = fa match {
      case FunctionCallExpression(name, args, t) => FunctionCallExpression(name, args.map(f), t)
      case Binary(left, right, operator, t)      => Binary(f(left), f(right), operator, t)
      case Unary(e, t)                           => Unary(f(e), t)
      case LiteralExpression(literal, t)         => LiteralExpression(literal, t)
      case FieldRef(value, t)                    => FieldRef(value, t)
    }

}

case class Fix[F[_]](val unfix: F[Fix[F]])

type Expression = Fix[ExpressionF]

sealed trait Source {

  def tableType: ComplexType.Table
}

object Source {
  val stdinType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("0" -> SimpleType.String))

  case object StdIn extends Source {
    def tableType = stdinType
  }

  case class TableRef(name: String, tableType: ComplexType.Table) extends Source

  case class SubQuery(statement: Statement.SelectStatement, alias: String) extends Source {
    def tableType = statement.tableType
  }

}

/** Statements
  */
case class FromSource(source: Source, joinPredicates: Option[Expression]) {
  def tableType = source.tableType
}

case class FromClause(items: NonEmptyList[FromSource])
case class WhereClause(expression: Expression)

case class Projection(e: Expression, maybeAlias: Option[String])

enum Statement {
  import cats.data.NonEmptyList

  case SelectStatement(
    projections: NonEmptyList[Projection],
    fromClause: FromClause,
    whereClause: Option[WhereClause] = None,
    tableType: ComplexType.Table
  )

}

import cats.data.StateT
import cats.data.EitherT

type Verified[K] = StateT[[A] =>> Either[String, A], Map[String, ComplexType.Table], K]

object Verified {
  def read: Verified[Map[String, ComplexType.Table]] = StateT.get
  def set(s: Map[String, ComplexType.Table]): Verified[Unit] = StateT.set(s)
  def pure[K](k: K): Verified[K] = StateT.pure(k)
  def error(s: String): Verified[Nothing] = s.asLeft.liftTo[Verified]
}

// the Type I want is Either[String, State[Map[..], K]]
def analyzeExpression(
  e: cst.Expression
): Verified[Fix[ExpressionF]] = e match {
  case cst.Expression.LiteralExpression(l)                    =>
    val expressionType = l match {
      // TODO: actually depending on the regex literal we can either
      // return an array or a string, that is the goal of it :)
      case cst.Literal.RegexLiteral(_)  => Type.String
      case cst.Literal.StringLiteral(_) => Type.String
      case cst.Literal.NumberLiteral(_) => Type.Number
      case cst.Literal.BoolLiteral(_)   => Type.Bool
    }

    StateT.pure(Fix(ExpressionF.LiteralExpression(l, expressionType)))
  case cst.Expression.FunctionCallExpression(name, arguments) =>
    val function = Function.valueOf(name)

    for {
      analyzedArguments <- arguments.traverse(analyzeExpression(_))
      zippedArgsWithExpectedTypes = function.args.zip(analyzedArguments)
      _                 <-
        zippedArgsWithExpectedTypes
          .foldM(()) { case (_, (expectedArgType, Fix(analyzedExpression))) =>
            analyzedExpression.expressionType match {
              case `expectedArgType` => Either.unit
              case _                 =>
                s"Error in function $name, argument with type ${analyzedExpression.expressionType} expected to have type $expectedArgType".asLeft
            }
          }
          .liftTo[Verified]
    } yield Fix(ExpressionF.FunctionCallExpression(name, analyzedArguments, function.returnType))

  case cst.Expression.Binary(left, right, operator) =>
    for {
      leftA      <- analyzeExpression(left)
      rightA     <- analyzeExpression(right)
      leftType = leftA.unfix.expressionType
      rightType = rightA.unfix.expressionType
      commonType <- {
        if (leftType == rightType) leftA.unfix.expressionType.asRight
        else
          s"Types on the LHS and RHS should be the same for operator ${operator.toString}, here found ${leftType.toString} and ${rightType.toString}".asLeft
      }.liftTo[Verified]
      resultType = (commonType, operator) match {
                     // Equality is valid for every type
                     case (_, Operator.Equal) | (_, Operator.Different)           =>
                       Either.right(Type.Bool)
                     // Int specific comparisons
                     case (Type.Number, o)
                         if o == Operator.Less || o == Operator.More || o == Operator.LessEqual || o == Operator.MoreEqual =>
                       Either.right(Type.Bool)
                     // Int specific operations
                     case (Type.Number, o)
                         if o == Operator.Plus || o == Operator.Minus || o == Operator.Divided || o == Operator.Times =>
                       Either.right(Type.Number)
                     // Only string operator is concatenation
                     case (Type.String, Operator.Plus)                            => Either.right(Type.String)
                     case (Type.Bool, o) if o == Operator.And || o == Operator.Or => Either.right(Type.Bool)
                     case _ => s"Operator ${operator.toString} cannot be used with type ${leftType.toString}".asLeft
                   }
      r          <- resultType.liftTo[Verified]
    } yield Fix(ExpressionF.Binary(leftA, rightA, operator, r))
  case cst.Expression.Unary(e)                      =>
    "not yet implemented".asLeft.liftTo[Verified]

  // identifier is only a field identifier for now as CTE are not supported
  case cst.Expression.IdentifierExpression(identifier) =>
    def getTable(tableId: String): Verified[ComplexType.Table] =
      for {
        state  <- Verified.read
        tableId :: field :: _ = identifier.split('.').toList
        foundTableType = state.get(tableId) match {
                           case Some(tableType) =>
                             tableType.asRight
                           case None            => s"Table not found $tableId".asLeft
                         }
        result <- foundTableType.liftTo[Verified]
      } yield result

    for {
      fqdn   <- Verified.pure(identifier.split("[.]").toList)
      result <- fqdn match {
                  // case tableId :: Nil            => "table references not supported ($tableId)".asLeft.liftTo[Verified]
                  case tableId :: fieldId :: Nil =>
                    getTable(tableId).flatMap { tableType =>
                      tableType.cols(fieldId) match {
                        case None            => s"Non existing field $fieldId on table $tableId".asLeft.liftTo[Verified]
                        case Some(fieldType) =>
                          Fix(ExpressionF.FieldRef(identifier, Type.Simple(fieldType))).asRight.liftTo[Verified]
                      }
                    }
                  case _                         =>
                    // weird
                    // This works:
                    // val e = "lol".asLeft
                    // var r = e.liftTo[Verified]
                    // r
                    //
                    // This does not:
                    // "lol".asLeft.liftTo[Verified]
                    Verified.error(s"only a.b identifier are supported, found $identifier")
                }
    } yield result
}

def analyzeStatement(
  s: cst.Statement.SelectStatement
): Verified[Statement.SelectStatement] =
  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("foo" -> SimpleType.Number))

  def analyzeSource(f: cst.Source): Verified[ast.Source] = {
    f match {
      case cst.Source.StdIn                      => Source.StdIn.asRight.liftTo[Verified]
      case cst.Source.TableRef(id)               =>
        for {
          ref    <- Verified.read
          source <- (ref.get(id) match {
                      case Some(tableType) => Source.TableRef(id, tableType).asRight
                      case None            => s"table not found $id".asLeft
                    }).liftTo[Verified]
        } yield source
      case cst.Source.SubQuery(statement, alias) =>
        for {
          s     <- analyzeStatement(statement)
          state <- Verified.read
          _     <- Verified.set(state + (alias -> s.tableType))
        } yield Source.SubQuery(s, alias)
    }
  }

  def analyzeFromSource(fromItem: cst.FromSource): Verified[ast.FromSource] = {
    for {
      analyzedSource          <- analyzeSource(fromItem.source)
      maybeAnalyzedExpression <- fromItem
                                   .maybeJoinPredicates
                                   .traverse(analyzeExpression(_))
    } yield FromSource(analyzedSource, maybeAnalyzedExpression)
  }

  def analyzeFromClause(f: cst.FromClause): Verified[ast.FromClause] = {
    for {
      analyzedItems <-
        f.items
          .traverse(analyzeFromSource)
    } yield FromClause(analyzedItems)
  }

  s match {
    case cst.Statement.SelectStatement(projections, from, maybeWhere) =>
      for {
        analyzedFrom        <- from match {
                                 case None    =>
                                   FromClause(NonEmptyList.one(FromSource(Source.StdIn, None))).asRight.liftTo[Verified]
                                 case Some(f) => analyzeFromClause(f)
                               }
        analyzedProjections <- projections.traverse { case cst.Projection(expression, alias) =>
                                 analyzeExpression(expression).map(e => Projection(e, alias))
                               }
        analyzedWhere <- maybeWhere.traverse(where => analyzeExpression(where.expression).map(k => WhereClause(k)))
        types = analyzedProjections
                  .zipWithIndex
                  .map { case (Projection(e, maybeAlias), index) =>
                    // TODO check that aliases are not repeated
                    val name = maybeAlias.getOrElse(index.toString)
                    val expressionType = e.unfix.expressionType match {
                      case Type.Simple(t) => t
                      case _              => sys.error("Expressions should return only simple types")
                    }

                    name -> expressionType
                  }
                  .toNem
      } yield Statement.SelectStatement(
        analyzedProjections,
        analyzedFrom,
        analyzedWhere,
        ComplexType.Table(types)
      )
  }
