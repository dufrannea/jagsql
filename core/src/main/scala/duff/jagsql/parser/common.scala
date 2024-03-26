package duff.jagsql
package parser

import duff.jagsql.cst.Position

import cats.{CoflatMap, Functor}
import cats.parse.Parser
import cats.parse.Parser.void

case class Indexed[+T](value: T, pos: Position) {
  def +[A](other: Indexed[A]): Indexed[(T, A)] = Indexed((value, other.value), pos.join(other.pos))
}

object Indexed {

  given CoflatMap[Indexed] = new CoflatMap[Indexed] {
    override def map[A, B](fa: Indexed[A])(f: A => B): Indexed[B] =
      fa.copy(value = f(fa.value))

    def coflatMap[A, B](fa: Indexed[A])(f: Indexed[A] => B): Indexed[B] =
      Functor[Indexed].map(fa)(_ => f(fa))

  }

}

val whitespace: Parser[Unit] = Parser.charIn(" \t\r\n").void
val lineComment = Parser.string("--") *> Parser.charsWhile(_ != '\n')

val w: Parser[Unit] = whitespace.rep.void *> (lineComment *> whitespace.rep0).rep0.void

case class Keyword(s: String)

// parsing rules
def keyword(s: String) =
  Parser
    .ignoreCase(s)
    .map(_ => Keyword(s))
    .indexed

val SELECT: IndexedParser[Keyword] = keyword("SELECT")
val FROM: IndexedParser[Keyword] = keyword("FROM")
val WHERE: IndexedParser[Keyword] = keyword("WHERE")
val JOIN: IndexedParser[Keyword] = keyword("JOIN")
val STDIN = keyword("STDIN")
val DUAL = keyword("DUAL")

val ON = keyword("ON")

val star = Parser.char('*').indexed
type IndexedParser[T] = Parser[Indexed[T]]

given indexedParserFunctor: Functor[IndexedParser] = new Functor[IndexedParser] {

  def map[A, B](fa: IndexedParser[A])(f: A => B): IndexedParser[B] = fa.map { case Indexed(value, pos) =>
    Indexed(f(value), pos)
  }

}

extension [K](p: IndexedParser[K]) {

  def unindexed: Parser[K] = p.map(_.value)

  //
  // F[A] -> (F[A] ->  B) => F[B]
  // that's *not* comonad, is it were f would need to be IndexedParser => B ?
  def emap[B](f: Indexed[K] => B): IndexedParser[B] = p.map { case v @ Indexed(value, pos) =>
    Indexed(f(v), pos)
  }

}

extension [T](p: Parser[T]) {

  def indexed: IndexedParser[T] = (Parser.index.with1 ~ p ~ Parser.index).map { case ((from, what), to) =>
    Indexed(what, Position(from, to))
  }

}
