package duff.jagsql
package ast

import cats.data.NonEmptyMap

object Type {
  val Number = SimpleType.Number
  val String = SimpleType.String
  val Bool = SimpleType.Bool

  def Table(cols: NonEmptyMap[String, SimpleType]) = ComplexType.Table(cols)
}

sealed trait Type

enum SimpleType extends Type {
  case Number
  case String
  case Bool
}

enum ComplexType extends Type {
  case Table(cols: NonEmptyMap[String, Type])
  case Array(typeParameter: Type)
}
