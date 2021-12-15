package io

import cats.Functor
import scodec.{Attempt, DecodeResult}

package object savro {
  final case class DecodeResultT[F[_], A](value: F[DecodeResult[A]]) {
    def map[B](f: A => B)(implicit F: Functor[F]): DecodeResultT[F, B] =
      DecodeResultT(F.map(value)(_.map(f)))
  }

  implicit val attemptFunctor: Functor[Attempt] = new Functor[Attempt] {
    override def map[A, B](fa: Attempt[A])(f: A => B): Attempt[B] = fa.map(f)
  }
}
