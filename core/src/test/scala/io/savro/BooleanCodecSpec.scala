package io.savro

import io.savro.AvroTypeCodecs.booleanCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class BooleanCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded boolean" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeBoolean(true)
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      booleanCodec.decode(bit) mustEqual successful(DecodeResult(true, BitVector.empty))
    }
    "avro java encoded booelan must be decodable by java avro" in {
      val bits = booleanCodec.encode(true).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readBoolean() mustEqual true
    }
  }

  "decode" must {
    "fail on not enough input" in {
      val bits = BitVector.fill(4)(high = false)
      booleanCodec.decode(bits) mustBe failure(Err.InsufficientBits(7, 4, List.empty[String]))
    }
    "fail on unexpected input" in {
      val bits = hex"0xf".toBitVector
      booleanCodec.decode(bits) mustBe failure(Err.General("expected constant BitVector(7 bits, 0x00) but got BitVector(7 bits, 0x0e)", List.empty[String]))
    }
    "decode zero to false" in {
      val bits = hex"0x0".toBitVector
      booleanCodec.decode(bits) mustBe successful(DecodeResult(false, BitVector.empty))
    }
    "decode one to true" in {
      val bits = hex"0x1".toBitVector
      booleanCodec.decode(bits) mustBe successful(DecodeResult(true, BitVector.empty))
    }
  }
  "encode" must {
    "encode true" in {
      val expectedBits = hex"0x01".toBitVector
      booleanCodec.encode(true) mustBe successful(expectedBits)
    }
    "encode false" in {
      val expectedBits = hex"0x00".toBitVector
      booleanCodec.encode(false) mustBe successful(expectedBits)
    }
  }
}
