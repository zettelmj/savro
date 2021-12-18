package io.savro

import scodec._
import scodec.bits.{BitVector, ByteVector, HexStringSyntax}
import scodec.codecs._
import shapeless.{::, HNil}

import java.nio.charset.Charset
import scala.annotation.tailrec

object AvroTypeCodecs {

  implicit val nullCodec: Codec[Unit] = new Codec[Unit] {
    override def encode(value: Unit): Attempt[BitVector] = Attempt.successful(BitVector.empty)

    override def sizeBound: SizeBound = SizeBound.exact(size = 0)

    override def decode(bits: BitVector): Attempt[DecodeResult[Unit]] =
      Attempt.successful(DecodeResult((), bits))
  }

  implicit val booleanCodec: Codec[Boolean] = new Codec[Boolean] {
    private val codec = constant(BitVector.fill(7)(high = false)) :~>: scodec.codecs.bits(1)

    override def decode(bits: BitVector): Attempt[DecodeResult[Boolean]] = {
      DecodeResultT(codec.decode(bits)).map {
        case value :: HNil => value.head
      }.value
    } // TODO: acquire bits before passing into individual codes to get nicer error message

    override def encode(value: Boolean): Attempt[BitVector] =
      codec.encode(BitVector.bit(value) :: HNil)

    override def sizeBound: SizeBound = SizeBound.exact(size = 8)
  }

  implicit val intCodec: Codec[Int] = new Codec[Int] {

    override def encode(value: Int): Attempt[BitVector] = {
      @tailrec
      def encodeRec(number: Int, recursionDepth: Int, cur: BitVector): BitVector = {
        if (recursionDepth == 4) {
          cur
        } else {
          if (number <= 0x7f) {
            cur ++ BitVector.fromByte(number.toByte)
          } else {
            val n = cur ++ BitVector.fromByte(((number | 0x80) & 0xFF).toByte)
            encodeRec(number >>> 7, recursionDepth + 1, n)
          }
        }
      }

      val encodedValue = ZigZagEncoding.encode(value)

      val result = if ((encodedValue & ~0x7F) != 0) {
        val n = BitVector.fromByte(((encodedValue | 0x80) & 0xFF).toByte)
        encodeRec(encodedValue >>> 7, 0, n)
      } else {
        BitVector.fromByte((encodedValue & 0x7F).toByte)
      }.compact

      Attempt.successful(result)
    }

    override def sizeBound: SizeBound = SizeBound.bounded(lower = 8, upper = 32)

    override def decode(bits: BitVector): Attempt[DecodeResult[Int]] = {
      def variableLengthDecoding(internalBits: BitVector, consumedBytes: Int, number: Int): Either[String, (Int, Int)] = {
        internalBits.acquire(8).flatMap { v =>
          val b = v.toByte() & 0xff
          val newNumber = number ^ ((b & 0x7f) << (consumedBytes * 7))
          val newConsumedBytes = consumedBytes + 1

          if (b <= 0x7f || newConsumedBytes == 5) {
            Right(newNumber, newConsumedBytes)
          } else {
            variableLengthDecoding(internalBits.drop(8), newConsumedBytes, newNumber)
          }
        }
      }

      variableLengthDecoding(bits, consumedBytes = 0, number = 0) match {
        case Right((number, consumedBytes)) =>
          Attempt.successful(DecodeResult(ZigZagEncoding.decode(number), bits.drop(consumedBytes * 8)))
        case Left(err) => Attempt.failure(Err.apply(err))
      }
    }
  }

  implicit val longCodec: Codec[Long] = new Codec[Long] {
    override def encode(value: Long): Attempt[BitVector] = {
      @tailrec
      def encodeRec(number: Long, recursionDepth: Int, cur: BitVector): BitVector = {
        if (recursionDepth == 9) {
          cur
        } else {
          if (number <= 0x7f) {
            cur ++ BitVector.fromByte(number.toByte)
          } else {
            val n = cur ++ BitVector.fromByte(((number | 0x80) & 0xFF).toByte)
            encodeRec(number >>> 7, recursionDepth + 1, n)
          }
        }
      }

      val encodedValue = ZigZagEncoding.encode(value)

      val result = if ((encodedValue & ~0x7F) != 0) {
        val n = BitVector.fromByte(((encodedValue | 0x80) & 0xFF).toByte)
        encodeRec(encodedValue >>> 7, 0, n)
      } else {
        BitVector.fromByte((encodedValue & 0x7F).toByte)
      }.compact

      Attempt.successful(result)

    }

    override def sizeBound: SizeBound = SizeBound.bounded(lower = 8, upper = 64)

    override def decode(bits: BitVector): Attempt[DecodeResult[Long]] = {
      def variableLengthDecoding(internalBits: BitVector, consumedBytes: Int, number: Long): Either[String, (Long, Int)] = {
        internalBits.acquire(8).flatMap { v =>
          val b = v.toByte() & 0xff
          val newNumber = number ^ ((b & 0x7f).toLong << (consumedBytes * 7))
          val newConsumedBytes = consumedBytes + 1

          if (b <= 0x7f || newConsumedBytes == 10) {
            Right(newNumber, newConsumedBytes)
          } else {
            variableLengthDecoding(internalBits.drop(8), newConsumedBytes, newNumber)
          }
        }
      }

      variableLengthDecoding(bits, consumedBytes = 0, number = 0) match {
        case Right((number, consumedBytes)) =>
          Attempt.successful(DecodeResult(ZigZagEncoding.decode(number), bits.drop(consumedBytes * 8)))
        case Left(err) => Attempt.failure(Err.apply(err))
      }
    }
  }

  implicit val floatCodec: Codec[Float] = new Codec[Float] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Float]] = floatL.decode(bits)

    override def encode(value: Float): Attempt[BitVector] = floatL.encode(value)

    override def sizeBound: SizeBound = SizeBound.exact(32)
  }

  implicit val doubleCodec: Codec[Double] = new Codec[Double] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Double]] = doubleL.decode(bits)

    override def encode(value: Double): Attempt[BitVector] = doubleL.encode(value)

    override def sizeBound: SizeBound = SizeBound.exact(64)
  }

  implicit val byteCodec: Codec[List[Byte]] = new Codec[List[Byte]] {
    private val codec = "length" | listOfN(intCodec, byte)

    override def encode(value: List[Byte]): Attempt[BitVector] = listOfN(intCodec, byte).encode(value)

    override def sizeBound: SizeBound = SizeBound.atLeast(8)

    override def decode(bits: BitVector): Attempt[DecodeResult[List[Byte]]] = codec.decode(bits)
  }

  implicit val stringCodec: Codec[String] = new Codec[String] {
    private val codec = ("length" | intCodec) >>:~ {
      length => "value" | fixedSizeBytes(length, string(Charset.defaultCharset())).hlist
    }

    override def encode(value: String): Attempt[BitVector] = for {
      length <- intCodec.encode(value.length)
      data <- fixedSizeBytes(value.length, string(Charset.defaultCharset())).encode(value)
    } yield (length ++ data).compact


    override def sizeBound: SizeBound = SizeBound.atLeast(longCodec.sizeBound.lowerBound)

    override def decode(bits: BitVector): Attempt[DecodeResult[String]] =
      DecodeResultT(codec.decode(bits)).map {
        case _ :: value :: HNil => value
      }.value
  }

  implicit def enumCodec[A](mapping: Int => A, inverseMapping: A => Int): Codec[A] = new Codec[A] {
    override def decode(bits: BitVector): Attempt[DecodeResult[A]] = intCodec.map(mapping).decode(bits)

    override def encode(value: A): Attempt[BitVector] = intCodec.encode(inverseMapping(value))

    override def sizeBound: SizeBound = SizeBound.atLeast(8)
  }

  implicit def arrayCodec[A](implicit innerCodec: Codec[A]): Codec[Seq[A]] = arrayCodec[A](itemsPerBlock = 1000)(innerCodec)

  implicit def arrayCodec[A](itemsPerBlock: Int)(implicit innerCodec: Codec[A]): Codec[Seq[A]] = new Codec[Seq[A]] {
    private val emptyByteCodec = constant(hex"0")
    private val terminationCodec = constant(ByteVector(0))
    private val blockCodec = listOfN(intCodec, innerCodec)

    // TODO: Implement reading blocks with byte size
    override def decode(bits: BitVector): Attempt[DecodeResult[Seq[A]]] = {
      def inner(b: BitVector, acc: Seq[A]): Attempt[DecodeResult[Seq[A]]] =
        peek(byte).decode(b).flatMap {
          case DecodeResult(0, _) => val correctionCodec = if (acc.isEmpty) emptyByteCodec else nullCodec
            (correctionCodec ~ terminationCodec).decode(b).map {
              r => DecodeResult(acc, r.remainder)
            }
          case _ => blockCodec.decode(b).flatMap {
            case DecodeResult(value, remainder) => inner(remainder, acc ++ value)
          }
        }

      DecodeResultT(inner(bits, Seq.empty[A])).value
    }

    // TODO: Implement writing blocks with byte size
    override def encode(value: Seq[A]): Attempt[BitVector] = {
      def inner(values: Seq[A], bits: BitVector): Attempt[BitVector] = {
        if (values.isEmpty) {
          val correctionCodec = if (bits.isEmpty) emptyByteCodec else nullCodec
          (correctionCodec ~ terminationCodec).encode((), ()).map(b => bits ++ b)
        } else {
          val (head, tail) = values.splitAt(itemsPerBlock)

          blockCodec.encode(head.toList).map(b => inner(tail, bits ++ b)).flatten
        }
      }

      inner(value, BitVector.empty)
    }

    override def sizeBound: SizeBound = SizeBound.atLeast(16)
  }

  implicit def mapCodec[A](implicit innerCodec: Codec[A]): Codec[Map[String, A]] = mapCodec[A](itemsPerBlock = 1000)

  implicit def mapCodec[A](itemsPerBlock: Int)(implicit innerCodec: Codec[A]): Codec[Map[String, A]] = new Codec[Map[String, A]] {
    private val emptyByteCodec = constant(hex"0")
    private val kvCodec = stringCodec :: innerCodec
    private val terminationCodec = constant(ByteVector(0))
    private val blockCodec = listOfN(intCodec, kvCodec)

    // TODO: Implement reading blocks with byte size
    override def decode(bits: BitVector): Attempt[DecodeResult[Map[String, A]]] = {
      def inner(b: BitVector, acc: List[String :: A :: HNil]): Attempt[DecodeResult[List[String :: A :: HNil]]] =
        peek(byte).decode(b).flatMap {
          case DecodeResult(0, _) =>
            val bNew = if (acc.isEmpty) b.drop(8) else b
            terminationCodec.decode(bNew).map(r => DecodeResult(acc, r.remainder))
          case _ => blockCodec.decode(b).flatMap {
            case DecodeResult(value, remainder) => inner(remainder, acc ++ value)
          }
        }

      DecodeResultT(inner(bits, List.empty)).map(l => l.map {
        case k :: v :: HNil => k -> v
      }.toMap).value
    }

    // TODO: Implement writing blocks with byte size
    override def encode(value: Map[String, A]): Attempt[BitVector] = {
      def inner(values: Map[String, A], bits: BitVector): Attempt[BitVector] = {
        if (values.isEmpty) {
          val correctionCodec = if (bits.isEmpty) emptyByteCodec else nullCodec
          (correctionCodec ~ terminationCodec).encode((), ()).map(b => bits ++ b)
        } else {
          val (head, tail) = values.splitAt(itemsPerBlock)

          blockCodec.encode(head.map {
            case (k, v) => k :: v :: HNil
          }.toList).map(b => inner(tail, bits ++ b)).flatten
        }
      }

      inner(value, BitVector.empty)
    }

    override def sizeBound: SizeBound = SizeBound.atLeast(16)
  }

  def unionCodec[A](mapping: Int => Codec[A], inverseMapping: A => Int): Codec[A] = new Codec[A] {
    override def decode(bits: BitVector): Attempt[DecodeResult[A]] = intCodec.flatPrepend { index =>
      mapping(index).hlist
    }.map {
      case _ :: result :: HNil => result
    }.decode(bits)

    override def encode(value: A): Attempt[BitVector] = (intCodec ~ mapping(inverseMapping(value))).encode(inverseMapping(value), value)

    override def sizeBound: SizeBound = SizeBound.atLeast(8)
  }

  def fixedCodec[A](size: Int)(implicit innerCodec: Codec[A]): Codec[Seq[A]] = new Codec[Seq[A]] {
    override def decode(bits: BitVector): Attempt[DecodeResult[Seq[A]]] = list(innerCodec).decode(bits)

    override def encode(value: Seq[A]): Attempt[BitVector] = list(innerCodec).encode(value.toList)

    override def sizeBound: SizeBound = SizeBound.exact(size)
  }
}
