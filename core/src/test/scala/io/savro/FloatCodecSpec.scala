package io.savro

import io.savro.AvroTypeCodecs.floatCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.DecodeResult
import scodec.bits.{BitVector, HexStringSyntax}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class FloatCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded float" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeFloat(0.5f)
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      floatCodec.decode(bit) mustEqual successful(DecodeResult(0.5f, BitVector.empty))
    }
    "avro java encoded float must be decodable by java avro" in {
      val bits = floatCodec.encode(0.5f).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readFloat() mustEqual 0.5f
    }
  }

  "encode" must {
    "0.0f into 00 00 00 00" in {
      val expectedBits = hex"00 00 00 00".toBitVector
      floatCodec.encode(0.0f) mustEqual successful(expectedBits)
    }
    "min float into ff ff 7f ff" in {
      val expectedBits = hex"ff ff 7f ff".toBitVector
      floatCodec.encode(Float.MinValue) mustEqual successful(expectedBits)
    }
    "max float into ff ff 7f 7f" in {
      val expectedBits = hex"ff ff 7f 7f".toBitVector
      floatCodec.encode(Float.MaxValue) mustEqual successful(expectedBits)
    }
  }
  "decode" must {
    "00 00 00 00 to 0.0f" in {
      val bits = hex"00 00 00 00".toBitVector
      floatCodec.decode(bits) mustEqual successful(DecodeResult(0.0f, BitVector.empty))
    }
    "ff ff 7f ff to min float" in {
      val bits = hex"ff ff 7f ff".toBitVector
      floatCodec.decode(bits) mustEqual successful(DecodeResult(Float.MinValue, BitVector.empty))
    }
    "ff ff 7f 7f to max float" in {
      val bits = hex"ff ff 7f 7f".toBitVector
      floatCodec.decode(bits) mustEqual successful(DecodeResult(Float.MaxValue, BitVector.empty))
    }
  }
}
