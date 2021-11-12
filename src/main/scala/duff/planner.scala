package duff.jagsql
package planner

import duff.jagsql.ast._

import cats._
import cats.data.NonEmptyList

enum Stage {
  case ReadStdIn
  case ReadFile(file: String)
  case Join(left: Stage, right: Stage, predicate: Option[Expression])
  case Projection(projections: NonEmptyList[ast.Projection], source: Stage)
  case Filter(predicate: Expression, source: Stage)

  case GroupBy(
    projections: NonEmptyList[ast.Projection],
    expressions: NonEmptyList[Expression],
    aggregations: NonEmptyList[Expression],
    source: Stage
  )

}

def toStage(source: ast.Source): Stage = source match {
  case Source.StdIn(_)                              => Stage.ReadStdIn
  case Source.SubQuery(statement, _)                => toStage(statement)
  case Source.TableRef(_, _)                        => sys.error("TableRef not supported")
  case Source.TableFunction("FILE", path :: Nil, _) => Stage.ReadFile(path.asInstanceOf[cst.Literal.StringLiteral].a)
  case _                                            => sys.error("Stage not implemented")
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
        case None                                 =>
          val projectionStage = Stage.Projection(projections, toStage(fromClause))
          val filterStage = whereClause match {
            case None                              => projectionStage
            case Some(ast.WhereClause(expression)) => Stage.Filter(expression, projectionStage)
          }
          filterStage
        case Some(ast.GroupByClause(expressions)) => Stage.GroupBy(projections, expressions, ???, toStage(fromClause))
      }
  }
