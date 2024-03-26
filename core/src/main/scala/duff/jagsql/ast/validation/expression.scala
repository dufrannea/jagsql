package duff.jagsql
package ast
package validation

import duff.jagsql.ast.*
import duff.jagsql.ast.ExpressionF.{Binary, FieldRef, FunctionCallExpression, LiteralExpression, Unary}
import duff.jagsql.cst.{Operator, Position}
import duff.jagsql.parser.*
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
    case cst.Literal.RegexLiteral(w)  => SimpleType.Regex(w)
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
      analyzedArguments <- arguments.traverse(analyzeExpression _)
      functionValidation =
        function.validateArgs(analyzedArguments.toList.map(_.fix)).leftMap { case (argIndex, message) =>
          val argWithError: Labeled[ExpressionF] = analyzedArguments(argIndex)
          TrackedError(
            argWithError.a,
            message
          )
        }
      returnType        <- functionValidation.liftTo[Verified]
    } yield Labeled(ExpressionF.FunctionCallExpression(function, analyzedArguments, returnType), e.pos)

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
  case cst.Expression.Unary(e, Indexed(operator, operatorPos))            =>
    for {
      z <- analyzeExpression(e)
      expressionType = z.unlabel.expressionType
      resultType = (expressionType, operator) match {
                     case (Type.Bool, Operator.Not) =>
                       Either.right(Type.Bool)
                     case _                         =>
                       TrackedError(
                         operatorPos,
                         s"Operator ${operator.toString} cannot be used with type ${expressionType.toString}"
                       ).asLeft
                   }
      r <- resultType.liftTo[Verified]
    } yield Labeled(ast.ExpressionF.Unary(z, operator, r), e.pos)

  case cst.Expression.StructAccess(inner, accessors) =>
    for {
      i <- analyzeExpression(inner)
      r <- analyzeStructCall(i, accessors.value)
    } yield r

  // identifier is only a field identifier for now as CTE are not supported
  case cst.Expression.IdentifierExpression(iid @ Indexed(identifier, _)) =>
    def getTable0(tableId: String): Verified[Option[ComplexType.Table]] =
      for {
        state  <- Verified.read
        tableId :: field :: _ = identifier.split('.').toList
        foundTableType = state.get(tableId)
        result <- Right(foundTableType).liftTo[Verified]
      } yield result

    def findFieldInTables(field: String): Verified[(String, Type)] =
      for {
        state  <- Verified.read
        maybeFound = state.toList.flatMap { case (tableName, tableType) =>
                       tableType.cols.toNel.toList.collect { case (`field`, fieldType) => tableName -> fieldType }
                     }
        tableNameAndFieldType = maybeFound match {
                                  case r :: Nil => r.asRight
                                  case Nil      => TrackedError(iid.pos, s"Identifier not found $field").asLeft
                                  case h :: t   => TrackedError(iid.pos, s"Ambiguous identifier $field").asLeft
                                }
        result <- tableNameAndFieldType.liftTo[Verified]
      } yield result

    for {
      fqdn   <- Verified.pure(identifier.split("[.]").toList)
      result <- fqdn match {
                  case fieldId :: Nil =>
                    findFieldInTables(fieldId).map { (tableId, fieldType) =>
                      Labeled(ExpressionF.FieldRef(tableId, fieldId, fieldType), e.pos)
                    }

                  case firstId :: secondId :: t =>
                    val field: Verified[(IndexedExpression, List[String])] = getTable0(firstId) flatMap {
                      case Some(tableType) =>
                        tableType.cols(secondId) match {
                          case None            =>
                            TrackedError(iid.pos, s"Non existing field $secondId on table $firstId")
                              .asLeft
                              .liftTo[Verified]
                          case Some(fieldType) =>
                            (Labeled(ExpressionF.FieldRef(firstId, secondId, fieldType), e.pos) -> t).pure[Verified]
                        }
                      case None            =>
                        findFieldInTables(firstId).map { (tableId, fieldType) =>
                          Labeled(ExpressionF.FieldRef(tableId, firstId, fieldType), e.pos) -> (secondId :: t)
                        }
                    }
                    field.map { case (e, l) => e -> l.toNel }.flatMap {
                      case (e, None)            => field.map(_._1)
                      case (e, Some(accessors)) => analyzeStructCall(e, accessors)
                    }

                  case _ =>
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

def analyzeStructCall(
  inner: IndexedExpression,
  args: NonEmptyList[String]
) =
  (inner -> args) match {
    case (expression, accessors) =>
      val z = expression.unlabel.expressionType

      val maybeType: Either[String, Type] =
        accessors.foldM(z) { case (currentType, currentSelector) =>
          currentType match {
            case SimpleType.Struct(values) =>
              values(currentSelector).toRight(s"Cannot access member $currentSelector of type ${currentType.show}")
            case _                         => Left(s"${currentType.show} type is not subscriptable")
          }
        }
      maybeType match {
        case Right(t)    =>
          Labeled(
            ExpressionF
              .StructAccess(expression, accessors, t),
            inner.a
          ).pure[Verified]
        case Left(error) =>
          TrackedError(inner.a, error).asLeft.liftTo[Verified]
      }

  }
