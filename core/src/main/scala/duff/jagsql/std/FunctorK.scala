package duff.jagsql
package std

import cats.{~>, Functor, Id}
import cats.arrow.FunctionK
import cats.implicits.*

import shapeless3.deriving.K11

/** A higher-kinded functor, mapping functors.
  *
  * Like:
  *   - https://hackage.haskell.org/package/functor-combinators-0.4.1.0/docs/Data-HFunctor.html
  *   - https://typelevel.org/cats-tagless/api/cats/tagless/FunctorK.html
  */
trait FunctorK[K[_[_]]] {

  def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: K[F]): K[G]

  def fmapK[F[_]: Functor, G[_]](fkf: F[K[F]], g: F ~> G): G[K[G]] =
    g(fkf.map(kf => mapK(g)(kf)))

  def nestedMap[F[_]: Functor, G[_], M[_]: Functor](s: M[F[K[F]]], g: F ~> G): M[G[K[G]]] =
    s.map(fmapK(_, g))

}

object FunctorK {
  import shapeless3.deriving.K11

  def apply[K[_[_]]: FunctorK]: FunctorK[K] = summon

  extension [K[_[_]]: FunctorK, F[_]: Functor](instance: K[F]) {
    def mapK[G[_]: Functor](g: F ~> G) = FunctorK[K].mapK(g)(instance)
  }

  extension [K[_[_]]: FunctorK, F[_]: Functor, M[_]: Functor](instance: M[F[K[F]]]) {
    def nestedMap[G[_]](g: F ~> G): M[G[K[G]]] =
      FunctorK[K].nestedMap(instance, g)
  }

  extension [K[_[_]]: FunctorK, F[_]: Functor](instance: F[K[F]]) {
    def emap[G[_]](g: F ~> G): G[K[G]] =
      FunctorK[K].fmapK(instance, g)
  }

}

/** Implementation of derivation, seems to work but is slow and not really understandable.
  */
trait Esoteric {

  given [T]: FunctorK[K11.Id[T]] with
    def mapK[A[_]: Functor, B[_]](f: ~>[A, B])(at: A[T]): B[T] = f(at)

  given nestedFunctorK[K[_[_]]: FunctorK, M[_]: Functor]: FunctorK[[x[_]] =>> M[x[K[x]]]] =
    new FunctorK[[x[_]] =>> M[x[K[x]]]] {
      override def mapK[F[_]: Functor, G[_]](g: F ~> G)(s: M[F[K[F]]]): M[G[K[G]]] =
        Functor[M].map(s)(fkf => FunctorK[[x[_]] =>> x[K[x]]].mapK(g)(fkf))
    }

  type Unapplied[M[_[_]]] = [F[_]] =>> F[M[F]]

  // Trick to be able to apply unappliedFunctorK
  extension [K[_[_]]: FunctorK, F[_]: Functor](instance: F[K[F]]) {
    def unapplied: Unapplied[K][F] = instance
  }

  given unappliedFunctorK[H[_[_]]: FunctorK]: FunctorK[Unapplied[H]] =
    new FunctorK[Unapplied[H]] {

      override def mapK[F[_]: Functor, G[_]](g: ~>[F, G])(s: Unapplied[H][F]): Unapplied[H][G] =
        g(Functor[F].map(s) { (hf: H[F]) =>
          FunctorK[H].mapK(g)(hf)
        })

    }

  object Derivation:

    given functorKGen[H[_[_]]](
      using inst: => K11.Instances[FunctorK, H]
    ): FunctorK[H] = new FunctorK[H] {

      override def mapK[A[_]: Functor, B[_]](f: ~>[A, B])(ha: H[A]): H[B] =
        inst.map(ha)(
          [t[_[_]]] => (ft: FunctorK[t], ta: t[A]) => ft.mapK(f)(ta)
        )

    }

    given [T]: FunctorK[K11.Const[T]] with
      def mapK[A[_]: Functor, B[_]](f: ~>[A, B])(t: T): T = t

    inline def derived[F[_[_]]](using gen: K11.Generic[F]): FunctorK[F] = Derivation.functorKGen

}
