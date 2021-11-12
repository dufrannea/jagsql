package duff.jagsql
package ast

import duff.jagsql.ast.*
import duff.jagsql.cst.Operator
import duff.jagsql.std.*

import scala.language.experimental
import scala.util.Try

import cats.*
import cats.data.*
import cats.implicits.*

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

def filter(expression: ast.Expression, predicate: ExpressionF[Boolean] => Boolean) = {
  def alg(e: ExpressionF[Boolean]): Boolean =
    predicate(e)
  cata(alg)(expression)
}

def analyzeExpression(
  e: cst.Expression
): Verified[Fix[ExpressionF]] = e match {
  case cst.Expression.LiteralExpression(l)                    =>
    val expressionType = getLiteralType(l)

    StateT.pure(Fix(ExpressionF.LiteralExpression(l, expressionType)))
  case cst.Expression.FunctionCallExpression(name, arguments) =>
    // TODO: overload resolution
    val function = Function.valueOf(name)

    for {
      analyzedArguments <- arguments.traverse(analyzeExpression(_))
      argsStream = function.args.to(LazyList) ++: function
                     .maybeVariadic
                     .map(t => LazyList.continually(t))
                     .getOrElse(LazyList.empty[Type])
      zippedArgsWithExpectedTypes =
        argsStream
          .zip(analyzedArguments)
          .toList
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
    } yield Fix(ExpressionF.FunctionCallExpression(function, analyzedArguments, function.returnType))

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
                          Fix(ExpressionF.FieldRef(tableId, fieldId, fieldType)).asRight.liftTo[Verified]
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
def addEmptyStdin(s: cst.Statement.SelectStatement): cst.Statement.SelectStatement =
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

def analyzeStatement(
  i: cst.Statement.SelectStatement
): Verified[Statement.SelectStatement] =
  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("foo" -> SimpleType.Number))
  val s = addEmptyStdin(i)

  def analyzeSource(f: cst.Source): Verified[ast.Source] =
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

  def validateFunction(source: cst.Source.TableFunction): Verified[Source] = {
    val fileFunType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.String))

    source match {
      case cst.Source.TableFunction("FILE", args, alias) if args.map(getLiteralType) == List(Type.String) =>
        Source.TableFunction("FILE", args, fileFunType).asRight.liftTo[Verified]
      case _ => s"Unknown table function ${source.name} with supplied arguments".asLeft.liftTo[Verified]
    }

  }

  def analyzeFromSource(fromItem: cst.FromSource): Verified[ast.FromSource] =
    for {
      analyzedSource          <- analyzeSource(fromItem.source)
      maybeAnalyzedExpression <- fromItem
                                   .maybeJoinPredicates
                                   .traverse(analyzeExpression(_))
    } yield FromSource(analyzedSource, maybeAnalyzedExpression)

  def analyzeFromClause(f: cst.FromClause): Verified[ast.FromClause] =
    for {
      analyzedItems <-
        f.items
          .traverse(analyzeFromSource)
    } yield FromClause(analyzedItems)

  def isAggregateFunction(e: ast.Expression): Boolean = {
    e.unfix match {
      case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, _, _) => true
      case _                                                                  => false
    }
  }

  def checkDoNotContainAggregateFunctions(
    expressions: NonEmptyList[ast.Expression]
  ): Verified[NonEmptyList[ast.Expression]] = {
    import cats.*
    import cats.implicits.*

    val isInvalid = expressions.exists(_.exists {
      case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, _, _) => true
      case _                                                                  => false
    })

    if (isInvalid)
      "Expressions in GROUP BY clause should not contain aggregation functions".asLeft.liftTo[Verified]
    else
      expressions.asRight.liftTo[Verified]

  }

  s match {
    case cst.Statement.SelectStatement(projections, from, maybeWhere, maybeGroupBy) =>
      for {
        // TODO: if FROM expansion appears at analysis, there is duplicate code
        // happening here
        analyzedFrom         <- from match {
                                  case None    => sys.error("should not happen, from clause has already been substituted")
                                  case Some(f) => analyzeFromClause(f)
                                }
        maybeAnalyzedGroupBy <-
          maybeGroupBy
            .traverse { case groupBy =>
              groupBy
                .expressions
                .traverse(analyzeExpression)
                // need to check the expressions are aggregate function free
                .flatMap(checkDoNotContainAggregateFunctions)
                .map(GroupByClause.apply)

            }
        analyzedProjections  <- projections.traverse { case cst.Projection(expression, alias) =>
                                  analyzeExpression(expression).map(e => Projection(e, alias))
                                }
        _                    <- maybeAnalyzedGroupBy
                                  .flatMap { g =>
                                    val allowedExpressions = g.expressions.toList.toSet

                                    val errors = analyzedProjections.collect {
                                      case projection
                                          if !allowedExpressions.contains(projection.e) && !isAggregateFunction(
                                            projection.e
                                          ) && !projection.e.unfix.isInstanceOf[ExpressionF.LiteralExpression[_]] =>
                                        s"$projection should be part of GROUP BY expressions, a literal or an aggregate function"
                                    }
                                    if (errors.isEmpty)
                                      None
                                    else
                                      Some(errors.mkString(","))
                                  }
                                  .toLeft(())
                                  .liftTo[Verified]
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
                    val expressionType: Type = e.unfix.expressionType match {
                      case t: SimpleType        => t
                      case t: ComplexType.Array => t
                      case _                    => sys.error("Expressions should return only simple types or ARRAY")
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
