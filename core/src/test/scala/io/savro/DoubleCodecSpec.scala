package io.savro

import io.savro.AvroTypeCodecs.doubleCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.DecodeResult
import scodec.bits.{BitVector, HexStringSyntax}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class DoubleCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded byte array" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeDouble(0.5d)
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      doubleCodec.decode(bit) mustEqual successful(DecodeResult(0.5f, BitVector.empty))
    }
    "avro java encoded double must be decodable by java avro" in {
      val bits = doubleCodec.encode(0.5d).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readDouble() mustEqual 0.5d
    }
  }

  "encode" must {
    "0.0f into 00 00 00 00 00 00 00 00" in {
      val expectedBits = hex"00 00 00 00 00 00 00 00".toBitVector
      doubleCodec.encode(0.0d) mustEqual successful(expectedBits)
    }
    "min double into ff ff ff ff ff ff ef ff" in {
      val expectedBits = hex"ff ff ff ff ff ff ef ff".toBitVector
      doubleCodec.encode(Double.MinValue) mustEqual successful(expectedBits)
    }
    "max double into ff ff ff ff ff ff ef 7f" in {
      val expectedBits = hex"ff ff ff ff ff ff ef 7f".toBitVector
      doubleCodec.encode(Double.MaxValue) mustEqual successful(expectedBits)
    }
  }
  "decode" must {
    "00 00 00 00 00 00 00 00 to 0.0f" in {
      val bits = hex"00 00 00 00 00 00 00 00".toBitVector
      doubleCodec.decode(bits) mustEqual successful(DecodeResult(0.0d, BitVector.empty))
    }
    "ff ff ff ff ff ff ef ff to min double" in {
      val bits = hex"ff ff ff ff ff ff ef ff".toBitVector
      doubleCodec.decode(bits) mustEqual successful(DecodeResult(Double.MinValue, BitVector.empty))
    }
    "ff ff ff ff ff ff ef 7f to max double" in {
      val bits = hex"ff ff ff ff ff ff ef 7f".toBitVector
      doubleCodec.decode(bits) mustEqual successful(DecodeResult(Double.MaxValue, BitVector.empty))
    }
  }

}
