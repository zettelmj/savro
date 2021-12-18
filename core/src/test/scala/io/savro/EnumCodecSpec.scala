package io.savro

import io.savro.AvroTypeCodecs.enumCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class EnumCodecSpec extends AnyWordSpec with Matchers {

  val testEnumerationMapping: Int => TestEnumeration = {
    case 0 => TestEnumeration.EntryA
    case 1 => TestEnumeration.EntryB
  }
  val testEnumerationInverseMapping: TestEnumeration => Int = {
    case TestEnumeration.EntryA => 0
    case TestEnumeration.EntryB => 1
  }

  sealed abstract class TestEnumeration()

  object TestEnumeration {
    case object EntryA extends TestEnumeration

    case object EntryB extends TestEnumeration
  }

  "interoperability" when {
    "decode" must {
      "decode java avro encoded enumeration" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeEnum(1)
        encoder.flush()

        enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
          .decode(BitVector(output.toByteArray)) mustEqual successful(DecodeResult(TestEnumeration.EntryB, BitVector.empty))
      }
    }
    "encode" must {
      "encode enumeration so that the java avro library can decode the value" in {
        val bits = enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
          .encode(TestEnumeration.EntryA).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readEnum() mustEqual 0
      }
    }
  }

  "encode" must {
    "encode EntryA to 00" in {
      val bits = enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
        .encode(TestEnumeration.EntryA).require

      bits mustEqual hex"00".toBitVector
    }
    "encode EntryB to 02" in {
      val bits = enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
        .encode(TestEnumeration.EntryB).require

      bits mustEqual hex"02".toBitVector
    }
  }
  "decode" must {
    "fail if not enough bits are available" in {
      val bits = BitVector.fromValidBin("0000")

      enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
        .decode(bits) mustEqual failure(Err(Err.insufficientBits(8, 4).toString()))
    }
    "decode 00 to EntryA" in {
      val bits = hex"00".toBitVector

      enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
        .decode(bits) mustEqual successful(DecodeResult(TestEnumeration.EntryA, BitVector.empty))
    }
    "decode 02 to EntryB" in {
      val bits = hex"02".toBitVector

      enumCodec[TestEnumeration](testEnumerationMapping, testEnumerationInverseMapping)
        .decode(bits) mustEqual successful(DecodeResult(TestEnumeration.EntryB, BitVector.empty))
    }
  }
}
