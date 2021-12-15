package io.savro

import io.savro.AvroTypeCodecs.intCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class IntCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" when {
    "decode" must {
      "be able to decode java avro encoded int 0" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(0)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(0, BitVector.empty))
      }
      "be able to decode java avro encoded int -64" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(-64)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(-64, BitVector.empty))
      }
      "be able to decode java avro encoded int 64" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(64)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(64, BitVector.empty))
      }
      "be able to decode java avro encoded int -8192" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(-8192)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(-8192, BitVector.empty))
      }
      "be able to decode java avro encoded int 8192" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(8192)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(8192, BitVector.empty))
      }
      "be able to decode java avro encoded int -1048576" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(-1048576)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(-1048576, BitVector.empty))
      }
      "be able to decode java avro encoded int 1048576" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(1048576)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(1048576, BitVector.empty))
      }
      "be able to decode java avro encoded int Int.MaxValue" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(Int.MaxValue)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(Int.MaxValue, BitVector.empty))
      }
      "be able to decode java avro encoded int Int.MinValue" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeInt(Int.MinValue)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        intCodec.decode(bit) mustEqual successful(DecodeResult(Int.MinValue, BitVector.empty))
      }
    }
    "encode" must {
      "avro java encoded int 0 must be decodable by java avro" in {
        val bits = intCodec.encode(0).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual 0
      }
      "avro java encoded int -64 must be decodable by java avro" in {
        val bits = intCodec.encode(-64).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual -64
      }
      "avro java encoded int 64 must be decodable by java avro" in {
        val bits = intCodec.encode(64).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual 64
      }
      "avro java encoded int -8192 must be decodable by java avro" in {
        val bits = intCodec.encode(-8192).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual -8192
      }
      "avro java encoded int 8192 must be decodable by java avro" in {
        val bits = intCodec.encode(8192).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual 8192
      }
      "avro java encoded int -10485576 must be decodable by java avro" in {
        val bits = intCodec.encode(-10485576).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual -10485576
      }
      "avro java encoded int 10485576 must be decodable by java avro" in {
        val bits = intCodec.encode(10485576).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual 10485576
      }
      "avro java encoded int Int.MaxValue must be decodable by java avro" in {
        val bits = intCodec.encode(Int.MaxValue).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual Int.MaxValue
      }
      "avro java encoded int Int.MinValue must be decodable by java avro" in {
        val bits = intCodec.encode(Int.MinValue).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readInt() mustEqual Int.MinValue
      }
    }
  }

  "decode" must {
    "fail on not enough input" in {
      val bits = BitVector.fill(4)(high = false)
      intCodec.decode(bits) mustBe failure(Err.General("cannot acquire 8 bits from a vector that contains 4 bits", List.empty))
    }
    "0x0 will decode to 0" in {
      val bits = hex"0x0".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(0, BitVector.empty))
    }
    "0x7F will decode to -64" in {
      val bits = hex"0x7F".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(-64, BitVector.empty))
    }
    "0x80 01 will decode to 64" in {
      val bits = hex"0x80 01".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(64, BitVector.empty))
    }
    "0xFF 7F will decode to -8192" in {
      val bits = hex"0xFF 7F".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(-8192, BitVector.empty))
    }
    "0x80 80 01 will decode to 8192" in {
      val bits = hex"0x80 80 01".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(8192, BitVector.empty))
    }
    "0xFF FF 7F will decode to -1048576" in {
      val bits = hex"0xFF FF 7F".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(-1048576, BitVector.empty))
    }
    "0x80 80 80 01 will decode to 1048576" in {
      val bits = hex"0x80 80 80 01".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(1048576, BitVector.empty))
    }
    "0xFE FF FF FF 0F will decode to max int" in {
      val bits = hex"0xFE FF FF FF 0F".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(Int.MaxValue, BitVector.empty))
    }
    "0xFF FF FF FF 0F will decode to min int" in {
      val bits = hex"0xFF FF FF FF 0F".toBitVector
      intCodec.decode(bits) mustBe successful(DecodeResult(Int.MinValue, BitVector.empty))
    }
  }
  "encode" must {
    "encoding 0 will result in 0x0" in {
      val expectedBits = hex"0x0".toBitVector
      intCodec.encode(0) mustBe successful(expectedBits)
    }
    "encoding -64 will result in 0x7F" in {
      val expectedBits = hex"0x7F".toBitVector
      intCodec.encode(-64) mustBe successful(expectedBits)
    }
    "encoding 64 will result in 0x80 01" in {
      val expectedBits = hex"0x80 01".toBitVector
      intCodec.encode(64) mustBe successful(expectedBits)
    }
    "encoding -8192 will result in 0xFF 7F" in {
      val expectedBits = hex"0xFF 7F".toBitVector
      intCodec.encode(-8192) mustBe successful(expectedBits)
    }
    "encoding 8192 will result in 0x80 80 01" in {
      val expectedBits = hex"0x80 80 01".toBitVector
      intCodec.encode(8192) mustBe successful(expectedBits)
    }
    "encoding -1048576 will result in 0xFF FF 7F" in {
      val expectedBits = hex"0xFF FF 7F".toBitVector
      intCodec.encode(-1048576) mustBe successful(expectedBits)
    }
    "encoding 1048576 will result in 0x80 80 80 01" in {
      val expectedBits = hex"0x80 80 80 01".toBitVector
      intCodec.encode(1048576) mustBe successful(expectedBits)
    }
    "encoding max int works" in {
      val expectedBits = hex"0xFE FF FF FF 0F".toBitVector
      intCodec.encode(Int.MaxValue) mustBe successful(expectedBits)
    }
    "encoding min int works" in {
      val expectedBits = hex"0xFF FF FF FF 0F".toBitVector
      intCodec.encode(Int.MinValue) mustBe successful(expectedBits)
    }
  }
}
