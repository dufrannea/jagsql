package duff.jagsql
package planner

import duff.jagsql.ast.*
import duff.jagsql.std.Fix

import cats.*
import cats.data.NonEmptyList
import cats.implicits.*

// TODO: can this be rewritten as a catamorphism ?
def getAggregateFunctions(a: ast.Expression): Set[ast.Expression] =
  a.unfix match {
    case ast.ExpressionF.FunctionCallExpression(f: AggregateFunction, args, name) =>
      Set(a)
    case r                                                                        =>
      r.foldMap(getAggregateFunctions)
  }

//def ralg(
//  expressions: Map[ast.Expression, String],
//  tableAlias: String
//)(
//  e: ExpressionF[Expression]
//): Expression =
//  expressions
//    .get(Fix(e)) match {
//    case Some(id) =>
//      println(s"Found expression $e")
//      Fix(ast.ExpressionF.FieldRef(tableAlias, id, e.expressionType))
//    case None     =>
//      println(s"Not found expression $e")
//      Fix(e)
//  }
//
// This replace function works inside-out
// so instead of replacing the outermost expressions
// it starts by replacing the innermost ones
// which is problematic for "nested replaces"
//def r2(expressions: Map[ast.Expression, String], tableAlias: String)(e: Expression) = std.cata(ralg(expressions, tableAlias))(e)

def r3(
  expressions: Map[ast.Expression, String],
  tableAlias: String
)(
  e: ast.Expression
): ast.Expression =
  expressions.get(e) match {
    case Some(id) =>
      Fix(ast.ExpressionF.FieldRef(tableAlias, id, e.unfix.expressionType))
    case None     =>
      Fix(e.unfix.map(r3(expressions, tableAlias)))
  }

//def replaceExpressionWithGroupByReference(
//  expressions: Map[ast.Expression, String],
//  tableAlias: String
//)(
//  e: ast.Expression
//): ast.Expression =
//  expressions
//    .get(e) match {
//    case Some(id) =>
//      println(s"Found expression $e")
//      Fix(ast.ExpressionF.FieldRef(tableAlias, id, e.unfix.expressionType))
//    case None     =>
//      println(s"Not found expression $e")
//      e
//  }
//
//def replaceGroupByExpressions(
//  groupByExpressions: Map[ast.Expression, String],
//  tableAlias: String
//)(
//  e: ast.Expression
//): ast.Expression =
//  Fix(e.unfix.map(replaceExpressionWithGroupByReference(groupByExpressions, tableAlias)))

enum Stage {
  case ReadStdIn
  case ReadDual
  case ReadFile(file: String)
  case Join(left: Stage, right: Stage, predicate: Option[Expression])
  case Projection(projections: NonEmptyList[ast.Projection], source: Stage)
  case Filter(predicate: Expression, source: Stage)

  case GroupBy(
    expressions: NonEmptyList[(Expression, String)],
    aggregations: List[(Expression, String)],
    source: Stage
  )

}

def toStage(source: ast.Source): Stage = source match {
  case Source.StdIn(_)                              => Stage.ReadStdIn
  case Source.Dual(_)                               => Stage.ReadDual
  case Source.SubQuery(statement, _)                => toStage(statement)
  case Source.TableRef(_, _)                        => sys.error("TableRef not supported")
  case Source.TableFunction("FILE", path :: Nil, _) => Stage.ReadFile(path.asInstanceOf[cst.Literal.StringLiteral].a)
  case other @ Source.TableFunction(_, _, _)        => sys.error(s"Stage not implemented $other")
}

def toStage(fromClause: ast.FromClause): Stage =
  fromClause
    .items
    .foldLeft(Option.empty[Stage]) { case (acc, currentFromItem) =>
      val currentStage = toStage(currentFromItem.source)
      acc match {
        case None    => Some(currentStage)
        case Some(a) =>
          Some(Stage.Join(a, currentStage, currentFromItem.joinPredicates))
      }
    }
    .get

def toStage(statement: Statement.SelectStatement): Stage =
  statement match {
    case Statement.SelectStatement(projections, fromClause, whereClause, groupByClause, _) =>
      groupByClause match {
        case None                          =>
          val projectionStage = Stage.Projection(projections, toStage(fromClause))
          val filterStage = whereClause match {
            case None                              => projectionStage
            case Some(ast.WhereClause(expression)) => Stage.Filter(expression, projectionStage)
          }
          filterStage
        case Some(ast.GroupByClause(keys)) =>
          val aggregates = projections
            .map(_.e)
            .toList
            .flatMap(getAggregateFunctions)

          val namedKeys = keys
            .zipWithIndex
            .map { case (e, index) => e -> s"col_$index" }

          val namedAggregations = aggregates
            .zip(keys.length to (aggregates.length + keys.length))
            .map { case (e, index) => e -> s"col_$index" }

          val colMap = (namedKeys ++ namedAggregations)
            .toList
            .toMap

          val tableAlias = "group_0"

          val substituedProjections = projections.map { p =>
            p.copy(e = r3(colMap, tableAlias)(p.e))
          }

          val sourceStage = toStage(fromClause)
          val filterStage = whereClause match {
            case None                              => sourceStage
            case Some(ast.WhereClause(expression)) => Stage.Filter(expression, sourceStage)
          }

          Stage.Projection(substituedProjections, Stage.GroupBy(namedKeys, namedAggregations, filterStage))
      }
  }
