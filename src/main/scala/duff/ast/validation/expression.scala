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
