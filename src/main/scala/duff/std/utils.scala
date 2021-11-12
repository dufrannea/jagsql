package duff.jagsql
package std

import cats.*
import cats.implicits.*

case class Fix[F[_]](val unfix: F[Fix[F]])

def cata[F[_]: Functor, K](alg: F[K] => K)(e: Fix[F]): K =
  alg(e.unfix.map(cata(alg)))
