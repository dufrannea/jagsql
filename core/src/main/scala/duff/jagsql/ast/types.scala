package duff.jagsql
package ast

import duff.jagsql.std.RegexWrapper

import cats.Show
import cats.data.NonEmptyMap

object Type {
  val Number = SimpleType.Number
  val String = SimpleType.String
  val Bool = SimpleType.Bool

  def Table(cols: NonEmptyMap[String, SimpleType]) = ComplexType.Table(cols)

  given Show[Type] with {

    override def show(t: Type): String = t match {
      case SimpleType.Number       => s"Number"
      case SimpleType.String       => s"String"
      case SimpleType.Bool         => s"Bool"
      case SimpleType.Regex(w)     => s"Regex(/${w.regex.regex}/)"
      case SimpleType.Struct(cols) =>
        cols.toNel.toList.map { case (key, t) => s"$key -> ${show(t)}" }.mkString("{", ",", "}")
      case SimpleType.Array(t)     =>
        s"Array<${show(t)}>"
      case ComplexType.Table(cols) =>
        cols.toNel.toList.map { case (key, t) => s"$key -> ${show(t)}" }.mkString("{", ",", "}")
    }

  }

}

sealed trait Type {
  def isCompatibleWith(s: Type): Boolean =
    this == s
}

sealed trait SimpleType extends Type

object SimpleType {
  case object Number extends SimpleType

  case object String extends SimpleType

  case object Bool extends SimpleType

  case class Regex(w: RegexWrapper) extends SimpleType {

    override def isCompatibleWith(s: Type): Boolean =
      s match {
        case Regex(_) => true
        case _        => false
      }

  }

  case class Struct(cols: NonEmptyMap[String, SimpleType]) extends SimpleType

  case class Array(typeParameter: SimpleType) extends SimpleType

}

enum ComplexType extends Type {

  override def isCompatibleWith(s: Type): Boolean =
    (this, s) match {
      case (Table(_), Table(_)) => true
      case _                    => false
    }

  case Table(cols: NonEmptyMap[String, SimpleType])
}
