package duff.jagsql
package ast

import duff.jagsql.ast.*
import duff.jagsql.ast.ExpressionF.{Binary, FieldRef, FunctionCallExpression, LiteralExpression, Unary}
import duff.jagsql.cst.{Indexed, Operator, Position}
import duff.jagsql.std.*

import scala.language.experimental
import scala.util.Try

import cats.*
import cats.data.*
import cats.implicits.*

type Scope = Map[String, ComplexType.Table]

// This type is isomorphic to
// Either[TrackedError, State[Scope, K]]
//
// The State is used to remember previously seen
// table and columns resolutions
type Verified[K] = StateT[[A] =>> Either[TrackedError, A], Scope, K]

object Verified {
  def read: Verified[Map[String, ComplexType.Table]] = StateT.get

  def set(s: Map[String, ComplexType.Table]): Verified[Unit] = StateT.set(s)

  def pure[K](k: K): Verified[K] = StateT.pure(k)

  def error(s: String)(p: Position): Verified[Nothing] = TrackedError(p, s).asLeft.liftTo[Verified]
}

case class TrackedError(index: Position, message: String) {

  private def underline(sql: String, index: Position, message: String) = {
    val lineIndexes: Vector[(Int, Int)] = sql
      .linesIterator
      .foldLeft((0, List.empty[(Int, Int)])) { case ((from, p), current) =>
        val nextIndex = from + current.length + 1
        (nextIndex, (from -> (current.length + 1)) :: p)
      }
      ._2
      .reverse
      .toVector

    val firstIndex = lineIndexes
      .zipWithIndex
      .collectFirst { case (k, i) if k._1 <= index.from => i }
      .getOrElse(0)

    val lastIndex = lineIndexes
      .zipWithIndex
      .collectFirst { case (k, i) if (k._1 + k._2) >= index.to => i }
      .getOrElse(lineIndexes.length)

    val formattedError = (firstIndex to lastIndex).map { k =>
      val (startIndex, length) = lineIndexes(k)

      val padding =
        if (index.from >= startIndex)
          index.from - startIndex
        else 0

      val len =
        if (index.to <= startIndex + length)
          index.to - padding - startIndex
        else
          Math.max(length - padding - 1, 0)

      val line = sql.substring(startIndex, Math.max(0, startIndex + length - 1))
      val underlined = (0 until padding).map(_ => " ").mkString + (0 until len).map(_ => "^").mkString

      line + "\n" + underlined
    }

    s"""ERROR: $message
       |
       |${formattedError.mkString("\n")}""".stripMargin

  }

  def format(sql: String) =
    underline(sql, index, message)

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
  e: Indexed[cst.Expression[Indexed]]
): Verified[ast.IndexedExpression] = e.value match {
  case cst.Expression.LiteralExpression(l)                    =>
    val expressionType = getLiteralType(l.value)

    StateT.pure(Labeled(ExpressionF.LiteralExpression(l.value, expressionType), e.pos))
  case cst.Expression.FunctionCallExpression(name, arguments) =>
    // TODO: overload resolution
    val function = Function.valueOf(name.value)

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
          .foldM(()) { case (_, (expectedArgType, Labeled(analyzedExpression, _))) =>
            analyzedExpression.expressionType match {
              case `expectedArgType` => Either.unit
              case _                 =>
                TrackedError(
                  e.pos,
                  s"Error in function $name, argument with type ${analyzedExpression.expressionType}" +
                    " expected to have type $expectedArgType"
                ).asLeft
            }
          }
          .map(Indexed(_, e.pos))
          .liftTo[Verified]
    } yield Labeled(ExpressionF.FunctionCallExpression(function, analyzedArguments, function.returnType), e.pos)

  case cst.Expression.Binary(left, right, Indexed(operator, operatorPos)) =>
    for {
      leftA      <- analyzeExpression(left)
      rightA     <- analyzeExpression(right)
      leftType = leftA.unlabel.expressionType
      rightType = rightA.unlabel.expressionType
      commonType <- {
        if (leftType == rightType) leftA.unlabel.expressionType.asRight
        else
          TrackedError(
            e.pos,
            s"Types on the LHS and RHS should be the same for operator ${operator.toString}, here found ${leftType.toString} and ${rightType.toString}"
          ).asLeft
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
                     case _                                                       =>
                       TrackedError(
                         operatorPos,
                         s"Operator ${operator.toString} cannot be used with type ${leftType.toString}"
                       ).asLeft
                   }
      r          <- resultType.liftTo[Verified]

    } yield Labeled(ast.ExpressionF.Binary(leftA, rightA, operator, r), e.pos)
  case cst.Expression.Unary(e)                                            =>
    TrackedError(e.pos, "not yet implemented").asLeft.liftTo[Verified]

  // identifier is only a field identifier for now as CTE are not supported
  case cst.Expression.IdentifierExpression(iid @ Indexed(identifier, _)) =>
    def getTable(tableId: String): Verified[ComplexType.Table] =
      for {
        state  <- Verified.read
        tableId :: field :: _ = identifier.split('.').toList
        foundTableType = state.get(tableId) match {
                           case Some(tableType) =>
                             tableType.asRight
                           case None            => TrackedError(iid.pos, s"Table not found $tableId").asLeft
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
                        case None            =>
                          TrackedError(iid.pos, s"Non existing field $fieldId on table $tableId")
                            .asLeft
                            .liftTo[Verified]
                        case Some(fieldType) =>
                          (Labeled(ExpressionF.FieldRef(tableId, fieldId, fieldType), e.pos)).asRight.liftTo[Verified]
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
                    Verified.error(s"only a.b identifier are supported, found $identifier")(iid.pos)
                }
    } yield result
}

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

  def containsOnlyStaticOrGroupedByExpressions(
    e: ast.Expression,
    groupByExpressions: NonEmptySet[ast.Expression]
  ): Boolean = ???

  /** Check that projections are valid with respect to group by expressions, that is to say they are either:
    *   - pure
    *   - aggregates of anything are ok as long as they do not contain aggregates themselves
    *   - non aggregates are okay only if composing grouped by expressions
    */
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

                                  // val errors = analyzedProjections.collect {
                                  //  case projection
                                  //      if !allowedExpressions.contains(projection.e) && !isAggregateFunction(
                                  //        projection.e
                                  //      ) && !projection.e.unfix.isInstanceOf[ExpressionF.LiteralExpression[_]] =>
                                  //    s"$projection should be part of GROUP BY expressions, a literal or an aggregate function"
                                  // }
                                  // if (errors.isEmpty)
                                  //  None
                                  // else
                                  //  Some(errors.mkString(","))
                                  }
                                  .toLeft(())
                                  .liftTo[Verified]
        analyzedWhere        <-
          maybeWhere.traverse(where => analyzeExpression(where.value.expression).map(k => WhereClause(k.fix)))
        maybeAnalyzedGroupBy <-
          maybeGroupBy
            .traverse { case groupBy =>
              groupBy.value.expressions.traverse(analyzeExpression) // .map(e => GroupByClause(e.map(_.value)))
            }
        types = analyzedProjections
                  .zipWithIndex
                  .map { z =>
                    val (Indexed(Projection(e, maybeAlias), _), _) = z._1
                    // TODO check that aliases are not repeated
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
        None,
        ComplexType.Table(types)
      )
  }
