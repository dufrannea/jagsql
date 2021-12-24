package duff.jagsql
package ast
package validation

import duff.jagsql.ast.*
import duff.jagsql.ast.ExpressionF.{Binary, FieldRef, FunctionCallExpression, LiteralExpression, Unary}
import duff.jagsql.cst.{Indexed, Operator, Position}
import duff.jagsql.std.*

import scala.language.experimental
import scala.util.Try

import cats.*
import cats.data.*
import cats.implicits.*

// TODO: this is a bad idea to operate at the CST level because we loose the track of indexes
// if a from clause is not present, consider we are selecting from STDIN
def addEmptyStdin(s: cst.Statement.SelectStatement[Indexed]): cst.Statement.SelectStatement[Indexed] =
  s match {
    case cst.Statement.SelectStatement(projections, None, maybeWhere, maybeGroupBy) =>
      cst
        .Statement
        .SelectStatement(
          projections,
          Some(
            Indexed(
              cst.FromClause(
                NonEmptyList
                  .one(
                    Indexed(
                      cst.FromSource(Indexed(cst.Source.StdIn(Indexed("in", Position.empty)), Position.empty), None),
                      Position.empty
                    )
                  )
              ),
              Position.empty
            )
          ),
          maybeWhere,
          maybeGroupBy
        )
    case _                                                                          => s
  }

def analyzeStatement(
  i: cst.Statement.SelectStatement[Indexed]
): Verified[Statement.SelectStatement] =
  val ss: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("foo" -> SimpleType.Number))
  val s = addEmptyStdin(i)

  def analyzeSource(f: cst.Source[Indexed]): Verified[ast.Source] =
    f match {
      // TODO: there should be no special case here
      // for STDIN, it should be no different from a subquery or a table ref
      case cst.Source.StdIn(Indexed(alias, _))                           =>
        for {
          state  <- Verified.read
          source <- Source.StdIn(alias).asRight.liftTo[Verified]
          _      <- Verified.set(state + (alias -> source.tableType))
        } yield source
      case cst.Source.TableRef(Indexed(id, tablePos), Indexed(alias, _)) =>
        for {
          ref    <- Verified.read
          source <- (ref.get(id) match {
                      case Some(tableType) => Source.TableRef(id, tableType).asRight
                      case None            => TrackedError(tablePos, s"table not found $id").asLeft
                    }).liftTo[Verified]
          _      <- Verified.set(ref + (alias -> source.tableType))
        } yield source
      case cst.Source.SubQuery(Indexed(statement, _), Indexed(alias, _)) =>
        for {
          s     <- analyzeStatement(statement)
          state <- Verified.read
          _     <- Verified.set(state + (alias -> s.tableType))
        } yield Source.SubQuery(s, alias)
      case s @ cst.Source.TableFunction(_, _, Indexed(alias, _))         =>
        for {
          state    <- Verified.read
          function <- validateFunction(s)
          _        <- Verified.set(state + (alias -> function.tableType))
        } yield function
    }

  def validateFunction(source: cst.Source.TableFunction[Indexed]): Verified[Source] = {
    val fileFunType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.String))

    source match {
      case cst.Source.TableFunction(Indexed("FILE", _), args, Indexed(alias, _))
          if args.map(x => getLiteralType(x.value)) == List(Type.String) =>
        Source.TableFunction("FILE", args.map(_.value), fileFunType).asRight.liftTo[Verified]
      case _ =>
        TrackedError(source.name.pos, s"Unknown table function ${source.name} with supplied arguments")
          .asLeft
          .liftTo[Verified]
    }

  }

  def analyzeFromSource(fromItem: Indexed[cst.FromSource[Indexed]]): Verified[ast.FromSource] =
    for {
      analyzedSource          <- analyzeSource(fromItem.value.source.value)
      maybeAnalyzedExpression <- fromItem
                                   .value
                                   .maybeJoinPredicates
                                   .traverse(analyzeExpression _)
    } yield FromSource(analyzedSource, maybeAnalyzedExpression.map(_.fix))

  def analyzeFromClause(f: cst.FromClause[Indexed]): Verified[ast.FromClause] =
    for {
      analyzedItems <-
        f.items
          .traverse(analyzeFromSource)
    } yield FromClause(analyzedItems)

  def isAggregateFunction(e: ast.Expression): Boolean =
    e.unfix match {
      case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, _, _) => true
      case _                                                                  => false
    }

  // aggregate function not containing other aggregate functions
  def isOneLevelAggregateFunction(e: ast.IndexedExpression): Boolean =
    e.unlabel match {
      case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, arguments, _) =>
        arguments.collectFirst(containAggregateFunction(_)).isEmpty
      case _                                                                          => false
    }

  def containAggregateFunction_(e: ast.IndexedExpression): Either[ast.IndexedExpression, Unit] =
    e.unlabel match {
      case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, _, _) =>
        Either.left(e)
      // TODO: it is suboptimal because there is no early exit
      case _                                                                  =>
        e
          .unlabel
          .traverse(k => containAggregateFunction_(k))
          .as(())
    }

  def containAggregateFunction(e: ast.IndexedExpression): Option[ast.IndexedExpression] =
    containAggregateFunction_(e).left.toOption

  def checkDoNotContainAggregateFunctions(
    expressions: NonEmptyList[ast.IndexedExpression]
  ): Verified[NonEmptyList[ast.Expression]] = {
    val maybeInvalidExpression: Option[ast.IndexedExpression] =
      expressions.collectFirstSome(e => containAggregateFunction(e))

    maybeInvalidExpression match {
      case Some(expression) =>
        TrackedError(expression.a, "Expressions in GROUP BY clause should not contain aggregation functions")
          .asLeft
          .liftTo[Verified]
      case _                =>
        expressions.map(_.fix).asRight.liftTo[Verified]
    }
  }

  /** Check that projections are valid with respect to group by expressions, that is to say they are either:
    *   - pure
    *   - aggregates of anything are ok as long as they do not contain aggregates themselves
    *   - non aggregates are okay only if composing grouped by expressions
    */
  // TODO: rename to extract aggregates or something
  // this need to be used in the Groupby clause
  def isValidWithRespectToGroupBy(
    e: ast.IndexedExpression,
    groupByExpressions: Set[ast.Expression]
  ): Either[String, List[ast.IndexedExpression]] =
    if (groupByExpressions.contains(e.fix)) {
      Right(e :: Nil)
    } else {
      e.unlabel match {
        case FunctionCallExpression(func: AggregateFunction, args, t) =>
          val invalid = args.collectFirstSome(containAggregateFunction)
          if (invalid.nonEmpty)
            Left(s"Cannot nest aggregate functions in ${invalid}")
          else
            Right(args.toList)
        case FunctionCallExpression(func, args, t)                    =>
          args
            .toList
            .flatTraverse(
              isValidWithRespectToGroupBy(_, groupByExpressions)
            )
        case Binary(left, right, operator, t)                         =>
          for {
            l <- isValidWithRespectToGroupBy(left, groupByExpressions)
            r <- isValidWithRespectToGroupBy(right, groupByExpressions)
          } yield (l ++ r)
        case LiteralExpression(literal, t)                            =>
          Right(Nil)
        case Unary(e, t)                                              => sys.error("Not supported")
        case _                                                        =>
          Left(s"Expression is not included in GROUP BY")
      }
    }

  s match {
    case cst.Statement.SelectStatement(projections, from, maybeWhere, maybeGroupBy) =>
      for {
        // TODO: if FROM expansion appears at analysis, there is duplicate code
        // happening here
        analyzedFrom         <- from match {
                                  case None => sys.error("should not happen, from clause has already been substituted")
                                  case Some(Indexed(f, _)) => analyzeFromClause(f)
                                }
        maybeAnalyzedGroupBy <-
          maybeGroupBy
            .traverse { case groupBy =>
              groupBy
                .value
                .expressions
                .traverse(analyzeExpression)
                // need to check the expressions are aggregate function free
                .flatMap(checkDoNotContainAggregateFunctions)
                .map(GroupByClause.apply)

            }
        analyzedProjections  <- projections
                                  .traverse { case Indexed(cst.Projection(expression, alias), p) =>
                                    analyzeExpression(expression).map(_ -> (alias, p))
                                  }
                                  .map { expressions =>
                                    expressions.map { case (e, (alias, p)) =>
                                      Indexed(Projection(e.fix, alias.map(_.value)), p) -> e
                                    }
                                  }
        _                    <- maybeAnalyzedGroupBy
                                  .flatMap { g =>
                                    val allowedExpressions = g.expressions.toList.toSet

                                    analyzedProjections.foldMap { case (projection, expression) =>
                                      isValidWithRespectToGroupBy(expression, allowedExpressions)
                                        .leftMap(error => TrackedError(expression.a, error))
                                    } match {
                                      case Left(error)     => Some(error)
                                      case Right(datasets) => None
                                    }

                                  }
                                  .toLeft(())
                                  .liftTo[Verified]
        analyzedWhere        <-
          maybeWhere.traverse(where => analyzeExpression(where.value.expression).map(k => WhereClause(k.fix)))
        types = analyzedProjections
                  .zipWithIndex
                  .map { z =>
                    val (Indexed(Projection(e, maybeAlias), _), _) = z._1
                    // TODO: check that aliases are not repeated
                    val name = maybeAlias.getOrElse(s"col_${z._2}")
                    val expressionType: Type = e.unfix.expressionType match {
                      case t: SimpleType        => t
                      case t: ComplexType.Array => t
                      case _                    => sys.error("Expressions should return only simple types or ARRAY")
                    }

                    name -> expressionType
                  }
                  .toNem
      } yield Statement.SelectStatement(
        analyzedProjections.map(_._1.value),
        analyzedFrom,
        analyzedWhere,
        maybeAnalyzedGroupBy,
        ComplexType.Table(types)
      )
  }
