package duff.jagsql
package std

import cats._
import cats.implicits._

case class Fix[F[_]](val unfix: F[Fix[F]])

def cata[F[_]: Functor, K](alg: F[K] => K)(e: Fix[F]): K =
  alg(e.unfix.map(cata(alg)))
