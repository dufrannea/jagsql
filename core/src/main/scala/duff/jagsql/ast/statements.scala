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

sealed trait Source {

  def tableType: ComplexType.Table
}

object Source {
  val stdinType: ComplexType.Table = ComplexType.Table(NonEmptyMap.of("col_0" -> SimpleType.String))

  case class StdIn(alias: String) extends Source {
    def tableType = stdinType
  }

  case class Dual(alias: String) extends Source {
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

// SELECT
//    -- pure expressions always accepted
//    1,
//    CAST(1 AS STRING),
//    1 * 42,
//    -- a.b is grouped by so any expression
//    -- based on it is alright without aggregation
//    a.b + 1,
//   -- this is legit
//    a.d - 1,
//    a.d - 1 + 1, -- this is hard, it could be parsed as Minus(a.d, Plus(1, 1)),
//                 -- making it impossible to identify the aggregate expression here
//                 -- maybe we should represent the expressions differently
//                 -- (associate operations as lists Plus(List(a,b,c,d)) instead of binaries
//    -- aggregate function are authorized
//    -- over arbitrary expressions and composed
//    -- into arbitrary expressions
//    MAX(a.c - 1) + 1
// (...)
// GROUP BY a.b, a.d - 1
//
// GroupByClause(groupBy: any expression that is not an aggregate)
//
// Stage(
//      -- the aggregates to compute along the way
//      aggregates: List[AggregateFunctionCall],
//      -- the expressions to group by
//      groupByExpressions: NonEmptyList[Expression],
//      -- the projections to compute from the aggregates and the
//      -- groupBy expressions
//      projections: NonEmptyList[Expression])
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
