package duff.jagsql
package eval

import duff.jagsql.ast.*
import duff.jagsql.cst.Literal
import duff.jagsql.std.*
import duff.jagsql.std.RegexWrapper

import scala.util.matching.Regex

import cats.*
import cats.data.{Kleisli, Reader}
import cats.implicits.*

trait SqlShow[V] {
  def sqlPrint(v: V): String
}

given SqlShow[Value] with {

  override def sqlPrint(v: Value): String = v match {
    case Value.VString(e)      => s"'$e'"
    case Value.VRegex(e)       => s"/$e/"
    case Value.VNumber(e)      => s"${e.toString}"
    case Value.VBoolean(e)     => s"${e.toString}"
    case Value.VArray(e)       =>
      e.map(sqlPrint).mkString("[", ",", "]")
    case Value.VStruct(values) =>
      values
        .map { case (key, value) =>
          s"'$key' -> ${sqlPrint(value)}"
        }
        .mkString("{", ", ", "}")
    case Error                 => "ERROR"
  }

}

object SqlShow {

  extension [k: SqlShow](v: k) {
    def sqlPrint: String = summon[SqlShow[k]].sqlPrint(v)
  }

}

enum Value {
  case VString(e: String)
  case VRegex(e: RegexWrapper)
  case VNumber(e: BigDecimal)
  case VBoolean(e: Boolean)
  case VArray(values: List[Value])
  case VStruct(values: Map[String, Value])
  case Error
}

import duff.jagsql.cst.Operator
import duff.jagsql.eval.Value.*

type ExpressionValue = Reader[Map[String, Value], Value]

def alg(e: ExpressionF[ExpressionValue]): ExpressionValue = e match {
  case ExpressionF.LiteralExpression(l, _)                                           =>
    val value = l match {
      case Literal.BoolLiteral(e)   => VBoolean(e)
      case Literal.StringLiteral(e) => VString(e)
      case Literal.NumberLiteral(e) => VNumber(e)
      case Literal.RegexLiteral(e)  => VRegex(e)
    }
    Kleisli(_ => value)
  case ExpressionF.Binary(left, right, operator, commonType)                         =>
    val v = ((left, right)).tupled.map { case (l, r) => (l, r, operator) }.map {
      case (left, right, Operator.Equal)                => VBoolean(left == right)
      case (left, right, Operator.Different)            => VBoolean(left != right)
      case (VNumber(i), VNumber(j), Operator.Less)      => VBoolean(i < j)
      case (VNumber(i), VNumber(j), Operator.More)      => VBoolean(i > j)
      case (VNumber(i), VNumber(j), Operator.LessEqual) => VBoolean(i <= j)
      case (VNumber(i), VNumber(j), Operator.MoreEqual) => VBoolean(j <= i)
      case (VNumber(i), VNumber(j), Operator.Plus)      => VNumber(i + j)
      case (VNumber(i), VNumber(j), Operator.Minus)     => VNumber(i - j)
      case (VNumber(i), VNumber(j), Operator.Times)     => VNumber(i * j)
      case (VNumber(i), VNumber(j), Operator.Divided)   => VNumber(i / j)
      // Only string operator is concatenation
      case (VString(i), VString(j), Operator.Plus)      => VString(i + j)
      case (VBoolean(i), VBoolean(j), Operator.And)     => VBoolean(i && j)
      case (VBoolean(i), VBoolean(j), Operator.Or)      => VBoolean(i || j)
      case _                                            => Error
    }
    v
  case ExpressionF.Unary(inner, operator, expressionType)                            =>
    inner.map {
      case VBoolean(v) => VBoolean(!v)
      case _           => Error
    }
  case ExpressionF.FunctionCallExpression(`array`, arguments, _)                     =>
    Kleisli { values =>
      val k = arguments.map { argument =>
        argument.run(values)
      }
      VArray(k.toList)
    }
  case ExpressionF.FunctionCallExpression(`struct`, arguments, _)                    =>
    arguments.sequence.map { argValues =>
      val structValue = argValues
        .grouped(2)
        .map {
          case VString(key) +: value +: _ => key -> value
          case _                          => sys.error("Wrong arguments for struct")
        }
        .toMap
      VStruct(structValue)
    }
  case ExpressionF.FunctionCallExpression(`to_int`, arguments, _)                    =>
    Kleisli { values =>
      val stringVal = arguments.head.run(values)
      VNumber(stringVal.asInstanceOf[Value.VString].e.toInt)
    }
  case ExpressionF.FunctionCallExpression(`run`, arguments, _)                       =>
    import sys.process._
    Kleisli { values =>
      val command = arguments.head.run(values).asInstanceOf[Value.VString].e
      val result = command.!!

      VString(result)
    }
  case ExpressionF.FunctionCallExpression(`regex`, arguments, _)                     =>
    Kleisli { values =>
      val regexVal = arguments.head.run(values).asInstanceOf[Value.VRegex].e.regex
      val stringVal = arguments(1).run(values).asInstanceOf[Value.VString].e

      regexVal.findPrefixMatchOf(stringVal) match {
        case Some(matcher) =>
          if (matcher.groupCount == 0)
            VArray(VString(matcher.group(0)) :: Nil)
          else
            VArray((1 to matcher.groupCount).toList.map(k => VString(matcher.group(k))))
        case None          => VArray(Nil)
      }
    }
  case ExpressionF.FunctionCallExpression(`regex_struct`, arguments, expressionType) =>
    Kleisli { values =>
      val regexVal = arguments.head.run(values).asInstanceOf[Value.VRegex].e.regex
      val stringVal = arguments(1).run(values).asInstanceOf[Value.VString].e

      val matches: List[Value.VString] = regexVal.findPrefixMatchOf(stringVal) match {
        case Some(matcher) =>
          if (matcher.groupCount == 0)
            VString(matcher.group(0)) :: Nil
          else
            (1 to matcher.groupCount).toList.map(k => VString(matcher.group(k)))
        case None          => Nil
      }

      val groups = arguments
        .head
        .run(values)
        .asInstanceOf[Value.VRegex]
        .e
        .groups
        .map { case (index, col) => col.getOrElse(s"group_$index") }
        .toList

      val groupValues = groups.zip(matches).toMap

      VStruct(groupValues)
    }
  case ExpressionF.FunctionCallExpression(`max`, _, _)                               =>
    sys.error(s"Missing implementation for function max")
  case ExpressionF.FunctionCallExpression(`file`, _, _)                              =>
    sys.error(s"Missing implementation for function file")
  case ExpressionF.FieldRef(tableId, fieldId, _)                                     =>
    Kleisli { values =>
      values.getOrElse(fieldId, sys.error(s"Key not found $fieldId in ${values.toString}"))
    }
  case ExpressionF.StructAccess(inner, members, _)                                   =>
    inner.map { structValue =>
      members.foldLeft(structValue) { case (p, c) =>
        assert(p.isInstanceOf[Value.VStruct], "Member access is valid only for structs")
        val Value.VStruct(values) = p
        values(c)
      }
    }
}

def eval(e: ast.Expression): ExpressionValue =
  cata(alg)(e)
