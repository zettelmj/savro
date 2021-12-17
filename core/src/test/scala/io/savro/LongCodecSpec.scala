package io.savro

import io.savro.AvroTypeCodecs.longCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class LongCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" when {
    "decode" must {
      "be able to decode java avro encoded Long 0" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(0)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(0, BitVector.empty))
      }
      "be able to decode java avro encoded Long -64" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-64)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-64, BitVector.empty))
      }
      "be able to decode java avro encoded Long 64" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(64)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(64, BitVector.empty))
      }
      "be able to decode java avro encoded Long -8192" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-8192)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-8192, BitVector.empty))
      }
      "be able to decode java avro encoded Long 8192" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(8192)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(8192, BitVector.empty))
      }
      "be able to decode java avro encoded Long -1048576" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-1048576)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-1048576, BitVector.empty))
      }
      "be able to decode java avro encoded Long 1048576" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(1048576)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(1048576, BitVector.empty))
      }
      "be able to decode java avro encoded Long -134217728" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-134217728)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-134217728, BitVector.empty))
      }
      "be able to decode java avro encoded Long 134217728" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(134217728)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(134217728, BitVector.empty))
      }
      "be able to decode java avro encoded Long -17179869184L" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-17179869184L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-17179869184L, BitVector.empty))
      }
      "be able to decode java avro encoded Long 17179869184L" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(17179869184L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(17179869184L, BitVector.empty))
      }
      "be able to decode java avro encoded Long -2199023255552" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-2199023255552L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-2199023255552L, BitVector.empty))
      }
      "be able to decode java avro encoded Long 2199023255552" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(2199023255552L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(2199023255552L, BitVector.empty))
      }
      "be able to decode java avro encoded Long -281474976710656" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-281474976710656L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-281474976710656L, BitVector.empty))
      }
      "be able to decode java avro encoded Long 281474976710656" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(281474976710656L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(281474976710656L, BitVector.empty))
      }
      "be able to decode java avro encoded Long -36028797018963968" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(-36028797018963968L)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(-36028797018963968L, BitVector.empty))
      }
      "be able to decode java avro encoded int Long.MaxValue" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(Long.MaxValue)
        encoder.flush()
        val bit = BitVector(output.toByteArray)

        longCodec.decode(bit) mustEqual successful(DecodeResult(Long.MaxValue, BitVector.empty))
      }
      "be able to decode java avro encoded int Long.MinValue" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeLong(Long.MinValue)
        encoder.flush()
        val bit = BitVector(output.toByteArray)
        longCodec.decode(bit) mustEqual successful(DecodeResult(Long.MinValue, BitVector.empty))
      }
    }
    "encode" must {
      "avro java encoded Long 0 must be decodable by java avro" in {
        val bits = longCodec.encode(0).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 0
      }
      "avro java encoded Long -64 must be decodable by java avro" in {
        val bits = longCodec.encode(-64).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -64
      }
      "avro java encoded Long 64 must be decodable by java avro" in {
        val bits = longCodec.encode(64).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 64
      }
      "avro java encoded Long -8192 must be decodable by java avro" in {
        val bits = longCodec.encode(-8192).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -8192
      }
      "avro java encoded Long 8192 must be decodable by java avro" in {
        val bits = longCodec.encode(8192).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 8192
      }
      "avro java encoded Long -10485576 must be decodable by java avro" in {
        val bits = longCodec.encode(-10485576).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -10485576
      }
      "avro java encoded Long 10485576 must be decodable by java avro" in {
        val bits = longCodec.encode(10485576).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 10485576
      }
      "avro java encoded Long -134217728 must be decodable by java avro" in {
        val bits = longCodec.encode(-134217728).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -134217728
      }
      "avro java encoded Long 134217728 must be decodable by java avro" in {
        val bits = longCodec.encode(134217728).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 134217728
      }
      "avro java encoded Long -17179869184 must be decodable by java avro" in {
        val bits = longCodec.encode(-17179869184L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -17179869184L
      }
      "avro java encoded Long 17179869184 must be decodable by java avro" in {
        val bits = longCodec.encode(17179869184L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 17179869184L
      }
      "avro java encoded Long -2199023255552 must be decodable by java avro" in {
        val bits = longCodec.encode(-2199023255552L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -2199023255552L
      }
      "avro java encoded Long 2199023255552 must be decodable by java avro" in {
        val bits = longCodec.encode(2199023255552L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 2199023255552L
      }
      "avro java encoded Long -281474976710656 must be decodable by java avro" in {
        val bits = longCodec.encode(-281474976710656L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -281474976710656L
      }
      "avro java encoded Long 281474976710656 must be decodable by java avro" in {
        val bits = longCodec.encode(281474976710656L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 281474976710656L
      }
      "avro java encoded Long -36028797018963968 must be decodable by java avro" in {
        val bits = longCodec.encode(-36028797018963968L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual -36028797018963968L
      }
      "avro java encoded Long 36028797018963968 must be decodable by java avro" in {
        val bits = longCodec.encode(36028797018963968L).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual 36028797018963968L
      }
      "avro java encoded int Long.MaxValue must be decodable by java avro" in {
        val bits = longCodec.encode(Long.MaxValue).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual Long.MaxValue
      }
      "avro java encoded int Long.MinValue must be decodable by java avro" in {
        val bits = longCodec.encode(Long.MinValue).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readLong() mustEqual Long.MinValue
      }
    }
  }
  "decode" must {
    "fail on not enough input" in {
      val bits = BitVector.fill(4)(high = false)
      longCodec.decode(bits) mustBe failure(Err.General("cannot acquire 8 bits from a vector that contains 4 bits", List.empty))
    }
    "0 will decode to 0" in {
      val bits = hex"0".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(0, BitVector.empty))
    }
    "7F will decode to -64" in {
      val bits = hex"7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-64, BitVector.empty))
    }
    "80 01 will decode to 64" in {
      val bits = hex"80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(64, BitVector.empty))
    }
    "FF 7F will decode to -8192" in {
      val bits = hex"FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-8192, BitVector.empty))
    }
    "80 80 01 will decode to 8192" in {
      val bits = hex"80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(8192, BitVector.empty))
    }
    "FF FF 7F will decode to -1048576" in {
      val bits = hex"FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-1048576, BitVector.empty))
    }
    "80 80 80 01 will decode to 1048576" in {
      val bits = hex"80 80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(1048576, BitVector.empty))
    }
    "FF FF FF 7F will decode to -134217728" in {
      val bits = hex"FF FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-134217728, BitVector.empty))
    }
    "80 80 80 80 01 will decode to 134217728" in {
      val bits = hex"80 80 80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(134217728, BitVector.empty))
    }
    "FF FF FF FF 7F will decode to -17179869184L" in {
      val bits = hex"FF FF FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-17179869184L, BitVector.empty))
    }
    "80 80 80 80 80 01 will decode to 17179869184L" in {
      val bits = hex"80 80 80 80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(17179869184L, BitVector.empty))
    }
    "FF FF FF FF FF 7F will decode to -2199023255552" in {
      val bits = hex"FF FF FF FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-2199023255552L, BitVector.empty))
    }
    "80 80 80 80 80 80 01 will decode to 2199023255552L" in {
      val bits = hex"80 80 80 80 80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(2199023255552L, BitVector.empty))
    }
    "FF FF FF FF FF FF 7F will decode to -281474976710656" in {
      val bits = hex"FF FF FF FF FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-281474976710656L, BitVector.empty))
    }
    "80 80 80 80 80 80 80 01 will decode to 281474976710656L" in {
      val bits = hex"80 80 80 80 80 80 80 01".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(281474976710656L, BitVector.empty))
    }
    "FF FF FF FF FF FF FF 7F will decode to -36028797018963968" in {
      val bits = hex"FF FF FF FF FF FF FF 7F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(-36028797018963968L, BitVector.empty))
    }
    "FE FF FF FF FF FF FF FF FF 0F will decode to max long" in {
      val bits = hex"FE FF FF FF FF FF FF FF FF 0F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(Long.MaxValue, BitVector.empty))
    }
    "FF FF FF FF FF FF FF FF FF 0F will decode to min long" in {
      val bits = hex"FF FF FF FF FF FF FF FF FF 0F".toBitVector
      longCodec.decode(bits) mustBe successful(DecodeResult(Long.MinValue, BitVector.empty))
    }
  }
  "encode" must {
    "encoding 0 will result in 0" in {
      val expectedBits = hex"0".toBitVector
      longCodec.encode(0) mustBe successful(expectedBits)
    }
    "encoding -64 will result in 7F" in {
      val expectedBits = hex"7F".toBitVector
      longCodec.encode(-64) mustBe successful(expectedBits)
    }
    "encoding 64 will result in 80 01" in {
      val expectedBits = hex"80 01".toBitVector
      longCodec.encode(64) mustBe successful(expectedBits)
    }
    "encoding -8192 will result in FF 7F" in {
      val expectedBits = hex"FF 7F".toBitVector
      longCodec.encode(-8192) mustBe successful(expectedBits)
    }
    "encoding 8192 will result in 80 80 01" in {
      val expectedBits = hex"80 80 01".toBitVector
      longCodec.encode(8192) mustBe successful(expectedBits)
    }
    "encoding -1048576 will result in FF FF 7F" in {
      val expectedBits = hex"FF FF 7F".toBitVector
      longCodec.encode(-1048576) mustBe successful(expectedBits)
    }
    "encoding 1048576 will result in 80 80 80 01" in {
      val expectedBits = hex"80 80 80 01".toBitVector
      longCodec.encode(1048576) mustBe successful(expectedBits)
    }
    "encoding -134217728 will result in FF FF FF 7F" in {
      val expectedBits = hex"FF FF FF 7F".toBitVector
      longCodec.encode(-134217728) mustBe successful(expectedBits)
    }
    "encoding 134217728 will result in 80 80 80 80 01" in {
      val expectedBits = hex"80 80 80 80 01".toBitVector
      longCodec.encode(134217728) mustBe successful(expectedBits)
    }
    "encoding -17179869184L will result in FF FF FF FF 7F" in {
      val expectedBits = hex"FF FF FF FF 7F".toBitVector
      longCodec.encode(-17179869184L) mustBe successful(expectedBits)
    }
    "encoding 17179869184L will result in 80 80 80 80 80 01" in {
      val expectedBits = hex"80 80 80 80 80 01".toBitVector
      longCodec.encode(17179869184L) mustBe successful(expectedBits)
    }
    "encoding -2199023255552L will result in FF FF FF FF FF 7F" in {
      val expectedBits = hex"FF FF FF FF FF 7F".toBitVector
      longCodec.encode(-2199023255552L) mustBe successful(expectedBits)
    }
    "encoding 2199023255552L will result in 80 80 80 80 80 80 01" in {
      val expectedBits = hex"80 80 80 80 80 80 01".toBitVector
      longCodec.encode(2199023255552L) mustBe successful(expectedBits)
    }
    "encoding -281474976710656L will result in FF FF FF FF FF FF 7F" in {
      val expectedBits = hex"FF FF FF FF FF FF 7F".toBitVector
      longCodec.encode(-281474976710656L) mustBe successful(expectedBits)
    }
    "encoding 281474976710656L will result in 80 80 80 80 80 80 80 01" in {
      val expectedBits = hex"80 80 80 80 80 80 80 01".toBitVector
      longCodec.encode(281474976710656L) mustBe successful(expectedBits)
    }
    "encoding -36028797018963968L will result in FF FF FF FF FF FF FF 7F" in {
      val expectedBits = hex"FF FF FF FF FF FF FF 7F".toBitVector
      longCodec.encode(-36028797018963968L) mustBe successful(expectedBits)
    }
    "encoding long MaxValue will result in FE FF FF FF FF FF FF FF FF 01" in {
      val expectedBits = hex"FE FF FF FF FF FF FF FF FF 01".toBitVector
      longCodec.encode(Long.MaxValue) mustBe successful(expectedBits)
    }
    "encoding long MinValue will result in FF FF FF FF FF FF FF FF FF 01" in {
      val expectedBits = hex"FF FF FF FF FF FF FF FF FF 01".toBitVector
      longCodec.encode(Long.MinValue) mustBe successful(expectedBits)
    }
  }
}
