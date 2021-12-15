package io.savro

import io.savro.AvroTypeCodecs._
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt._
import scodec.DecodeResult
import scodec.Err.{General, insufficientBits}
import scodec.bits.{BitVector, HexStringSyntax}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class StringCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded string" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeString("This is a test")
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      stringCodec.decode(bit) mustEqual successful(DecodeResult("This is a test", BitVector.empty))
    }
    "encoded string must be decodable by java avro" in {
      val bits = stringCodec.encode(s"This is a test").require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readString() mustEqual "This is a test"
    }
  }

  "encode" must {
    "convert empty string to 0x00" in {
      val expectedBits = hex"0x00".toBitVector
      stringCodec.encode("") mustEqual successful(expectedBits)
    }
    "convert hello world to 0x16 68 65 6c 6c 6f 20 77 6f 72 6c 64" in {
      val expectedBits = hex"0x16 68 65 6c 6c 6f 20 77 6f 72 6c 64".toBitVector
      stringCodec.encode("hello world") mustEqual successful(expectedBits)
    }
  }

  "decode" must {
    "fail on empty input" in {
      val bits = BitVector.empty
      stringCodec.decode(bits) mustEqual failure(General(insufficientBits(8, 0).toString(), List("length")))
    }
    "return empty string for input 0x00" in {
      val bits = hex"0x00".toBitVector
      stringCodec.decode(bits) mustEqual successful(DecodeResult("", BitVector.empty))
    }
    "return hello world for input 0x16 68 65 6c 6c 6f 20 77 6f 72 6c 64" in {
      val bits = hex"0x16 68 65 6c 6c 6f 20 77 6f 72 6c 64".toBitVector
      stringCodec.decode(bits) mustEqual successful(DecodeResult("hello world", BitVector.empty))
    }
  }
}
