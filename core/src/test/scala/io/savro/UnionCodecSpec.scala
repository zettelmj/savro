package io.savro

import io.savro.AvroTypeCodecs.{nullCodec, stringCodec, unionCodec}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{Codec, DecodeResult}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class UnionCodecSpec extends AnyWordSpec with Matchers {
  val optionCodecMappingInverse: Option[_] => Int = {
    case None => 0
    case Some(_) => 1
  }
  val optionCodecMapping: Int => Codec[Option[String]] = {
    case 0 => nullCodec.xmap(_ => None, _ => ())
    case 1 => stringCodec.xmap(s => Some(s), s => s.getOrElse(""))
  }

  "interoperability" when {
    "decode" must {
      "decode java avro encoded string (Union of null, string)" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeIndex(1)
        encoder.writeString("a")
        encoder.flush()

        BitVector(output.toByteArray) mustEqual hex"02 02 61".toBitVector

        unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
          .decode(BitVector(output.toByteArray)) mustEqual
          successful(DecodeResult(Some("a"), BitVector.empty))
      }
      "decode java avro encoded null (Union of null, string)" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeIndex(0)
        encoder.flush()

        BitVector(output.toByteArray) mustEqual hex"00".toBitVector

        unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
          .decode(BitVector(output.toByteArray)) mustEqual
          successful(DecodeResult(None, BitVector.empty))
      }
    }
    "encode" must {
      "encode Some(a) (as Union of null and string) so that the java avro library can encode it" in {
        val bits = unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
          .encode(Some("a")).require

        bits mustEqual hex"02 02 61".toBitVector

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readIndex() mustEqual 1
        decoder.readString() mustEqual "a"
      }
      "encode None (as Union of null and string) so that the java avro library can encode it" in {
        val bits = unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
          .encode(None).require

        bits mustEqual hex"00".toBitVector

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readIndex() mustEqual 0
      }
    }
  }
  "decode" must {
    "decode 02 02 61 as Some(a) (as Union of null and string)" in {
      val bits = hex"02 02 61".toBitVector

      unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
        .decode(bits) mustEqual successful(DecodeResult(Some("a"), BitVector.empty))
    }
    "decode 00 as None (as Union of null and string)" in {
      val bits = hex"00".toBitVector

      unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
        .decode(bits) mustEqual successful(DecodeResult(None, BitVector.empty))
    }
  }
  "encode" must {
    "encode Some(a) (as Union of null and string) as 02 02 61" in {
      val expectedBits = hex"02 02 61".toBitVector

      unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
        .encode(Some("a")) mustEqual successful(expectedBits)
    }
    "encode None (as Union of null and string) as 00" in {
      val expectedBits = hex"00".toBitVector

      unionCodec[Option[String]](optionCodecMapping, optionCodecMappingInverse)
        .encode(None) mustEqual successful(expectedBits)
    }
  }
}
