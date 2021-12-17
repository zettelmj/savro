package io.savro

import io.savro.AvroTypeCodecs.byteCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.DecodeResult
import scodec.Err.{General, insufficientBits}
import scodec.bits.{BitVector, HexStringSyntax}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class ByteCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded byte array" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeBytes(Array.fill(2)(64.toByte))
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      byteCodec.decode(bit) mustEqual successful(DecodeResult(List(64, 64), BitVector.empty))
    }
    "avro java encoded byte array must be decodable by java avro" in {
      val bits = byteCodec.encode(List.fill(2)(64.toByte)).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readBytes(None.orNull).array() mustEqual Array(64, 64)
    }
  }

  "encode" must {
    "encode empty byte array to 00" in {
      val expectedBits = hex"00".toBitVector
      byteCodec.encode(List.empty[Byte]) mustEqual successful(expectedBits)
    }
    "encode hello world to 04 40 40" in {
      val expectedBits = hex"04 40 40".toBitVector
      byteCodec.encode(List(64.toByte, 64.toByte)) mustEqual successful(expectedBits)
    }
  }

  "decode" must {
    "fail on empty input" in {
      val bits = BitVector.empty
      byteCodec.decode(bits) mustEqual failure(General(insufficientBits(8, 0).toString(), List("length")))
    }
    "return empty byte array for input 00" in {
      val bits = hex"00".toBitVector
      byteCodec.decode(bits) mustEqual successful(DecodeResult(List.empty[Byte], BitVector.empty))
    }
    "return array containing 64, 64 for input 04 40 40" in {
      val bits = hex"04 40 40".toBitVector
      byteCodec.decode(bits) mustEqual successful(DecodeResult(List(64.toByte, 64.toByte), BitVector.empty))
    }
  }
}
