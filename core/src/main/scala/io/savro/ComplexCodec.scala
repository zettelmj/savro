package io.savro

import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, DecodingContext, SizeBound}
import shapeless.ops.hlist.RightFolder
import shapeless.{::, HList, HNil, Poly2}

object ComplexCodec {

  val hnilCodec: Codec[HNil] = new Codec[HNil] {
    override def decode(bits: BitVector): Attempt[DecodeResult[HNil]] = Attempt.successful(DecodeResult(HNil, bits))

    override def encode(value: HNil): Attempt[BitVector] = Attempt.successful(BitVector.empty)

    override def sizeBound: SizeBound = SizeBound.exact(0)
  }

  def prepend[H, T <: HList](default: H, t: Codec[T])(implicit h: Codec[H]): Codec[H :: T] = new Codec[H :: T] {
    override def encode(value: H :: T): Attempt[BitVector] = Codec.encodeBoth(h, t)(value.head, value.tail)

    override def sizeBound: SizeBound = h.sizeBound + t.sizeBound

    override def decode(bits: BitVector): Attempt[DecodeResult[H :: T]] = (for {
      decodedH <- DecodingContext(h.decode)
      decodedT <- DecodingContext(t.decode)
    } yield decodedH :: decodedT).decode(bits)
  }

  def apply[L <: HList, M <: HList](l: L)(implicit folder: RightFolder.Aux[L, Codec[HNil], Prepend.type, Codec[M]]): Codec[M] =
    l.foldRight(hnilCodec)(Prepend)

  object Prepend extends Poly2 {
    implicit def prependToHListCodec[H, T <: HList](implicit c: Codec[H]) = at[H, Codec[T]]((a, b) => ComplexCodec.prepend(a, b))
  }

}


