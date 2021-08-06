package duff.jagsql
package ast

import cats._
import cats.data.NonEmptyList
import cats.data.NonEmptyMap
import cats.data.State
import cats.implicits._

import scala.language.experimental
import scala.util.Try

import std._
import cst.Operator

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

type Expression = Fix[ExpressionF]

sealed trait Source {

  def tableType: ComplexType.Table
}

object Source {
  val stdinType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.String))

  case class StdIn(alias: String) extends Source {
    def tableType = stdinType
  }

  case class TableRef(name: String, tableType: ComplexType.Table) extends Source

  case class SubQuery(statement: Statement.SelectStatement, alias: String) extends Source {
    def tableType = statement.tableType
  }

  case class TableFunction(name: String, args: List[cst.Literal], tableType: ComplexType.Table) extends Source

}

/** Statements
  */
case class FromSource(source: Source, joinPredicates: Option[Expression]) {
  def tableType = source.tableType
}

case class FromClause(items: NonEmptyList[FromSource])
case class WhereClause(expression: Expression)
case class GroupByClause(expressions: NonEmptyList[Expression])

case class Projection(e: Expression, maybeAlias: Option[String])

enum Statement {
  import cats.data.NonEmptyList

  case SelectStatement(
    projections: NonEmptyList[Projection],
    fromClause: FromClause,
    whereClause: Option[WhereClause] = None,
    groupByClause: Option[GroupByClause] = None,
    tableType: ComplexType.Table
  )

}

import cats.data.StateT
import cats.data.EitherT

type Scope = Map[String, ComplexType.Table]

// This type is isomorphic to
// Either[String, State[Scope, K]]
//
// The State is used to remember previously seen
// table and columns resolutions
type Verified[K] = StateT[[A] =>> Either[String, A], Scope, K]

object Verified {
  def read: Verified[Map[String, ComplexType.Table]] = StateT.get
  def set(s: Map[String, ComplexType.Table]): Verified[Unit] = StateT.set(s)
  def pure[K](k: K): Verified[K] = StateT.pure(k)
  def error(s: String): Verified[Nothing] = s.asLeft.liftTo[Verified]
}

def getLiteralType(l: cst.Literal) =
  l match {
    // TODO: actually depending on the regex literal we can either
    // return an array or a string, that is the goal of it :)
    case cst.Literal.RegexLiteral(_)  => Type.String
    case cst.Literal.StringLiteral(_) => Type.String
    case cst.Literal.NumberLiteral(_) => Type.Number
    case cst.Literal.BoolLiteral(_)   => Type.Bool
  }

def analyzeExpression(
  e: cst.Expression
): Verified[Fix[ExpressionF]] = e match {
  case cst.Expression.LiteralExpression(l)                    =>
    val expressionType = getLiteralType(l)

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
                          Fix(ExpressionF.FieldRef(fieldId, Type.Simple(fieldType))).asRight.liftTo[Verified]
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

// if a from clause is not present, consider we are selecting from STDIN
def addEmptyStdin(s: cst.Statement.SelectStatement): cst.Statement.SelectStatement = {
  s match {
    case cst.Statement.SelectStatement(projections, None, maybeWhere, maybeGroupBy) =>
      cst
        .Statement
        .SelectStatement(
          projections,
          Some(cst.FromClause(NonEmptyList.one(cst.FromSource(cst.Source.StdIn("in"), None)))),
          maybeWhere,
          maybeGroupBy
        )
    case _                                                                          => s
  }
}

def analyzeStatement(
  i: cst.Statement.SelectStatement
): Verified[Statement.SelectStatement] =
  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("foo" -> SimpleType.Number))
  val s = addEmptyStdin(i)

  def analyzeSource(f: cst.Source): Verified[ast.Source] = {
    f match {
      // TODO: there should be no special case here
      // for STDIN, it should be no different from a subquery or a table ref
      case cst.Source.StdIn(alias)                   =>
        for {
          state  <- Verified.read
          source <- Source.StdIn(alias).asRight.liftTo[Verified]
          _      <- Verified.set(state + (alias -> source.tableType))
        } yield source
      case cst.Source.TableRef(id, alias)            =>
        for {
          ref    <- Verified.read
          source <- (ref.get(id) match {
                      case Some(tableType) => Source.TableRef(id, tableType).asRight
                      case None            => s"table not found $id".asLeft
                    }).liftTo[Verified]
          _      <- Verified.set(ref + (alias -> source.tableType))
        } yield source
      case cst.Source.SubQuery(statement, alias)     =>
        for {
          s     <- analyzeStatement(statement)
          state <- Verified.read
          _     <- Verified.set(state + (alias -> s.tableType))
        } yield Source.SubQuery(s, alias)
      case s @ cst.Source.TableFunction(_, _, alias) =>
        for {
          state    <- Verified.read
          function <- validateFunction(s)
          _        <- Verified.set(state + (alias -> function.tableType))
        } yield function
    }
  }

  def validateFunction(source: cst.Source.TableFunction): Verified[Source] = {
    val fileFunType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.String))

    source match {
      case cst.Source.TableFunction("FILE", args, alias) if args.map(getLiteralType) == List(Type.String) =>
        Source.TableFunction("FILE", args, fileFunType).asRight.liftTo[Verified]
      case _ => s"Unknown table function ${source.name} with supplied arguments".asLeft.liftTo[Verified]
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
    case cst.Statement.SelectStatement(projections, from, maybeWhere, maybeGroupBy) =>
      for {
        // TODO: if FROM expansion appears at analysis, there is duplicate code
        // happening here
        analyzedFrom        <- from match {
                                 case None    => sys.error("should not happen, from clause has already been substituted")
                                 case Some(f) => analyzeFromClause(f)
                               }
        analyzedProjections <- projections.traverse { case cst.Projection(expression, alias) =>
                                 analyzeExpression(expression).map(e => Projection(e, alias))
                               }
        analyzedWhere <- maybeWhere.traverse(where => analyzeExpression(where.expression).map(k => WhereClause(k)))
        maybeAnalyzedGroupBy <-
          maybeGroupBy
            .traverse { case groupBy =>
              groupBy.expressions.traverse(analyzeExpression).map(GroupByClause.apply)
            }
        types = analyzedProjections
                  .zipWithIndex
                  .map { case (Projection(e, maybeAlias), index) =>
                    // TODO check that aliases are not repeated
                    val name = maybeAlias.getOrElse(s"col_$index")
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
        None,
        ComplexType.Table(types)
      )
  }
