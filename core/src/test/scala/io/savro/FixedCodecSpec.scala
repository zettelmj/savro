package io.savro

import io.savro.AvroTypeCodecs.fixedCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.codecs.byte
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class FixedCodecSpec extends AnyWordSpec with Matchers {

  "interoperability" when {
    "decode" must {
      "decode java avro encoded fixed sized complex type of length 4" in {

        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        val bits = hex"00 ff 00 ff"
        encoder.writeFixed(bits.toArray)
        encoder.flush()

        fixedCodec[Byte](size = 4)(byte).decode(BitVector(output.toByteArray)) mustEqual
          successful(DecodeResult(bits.toArray.toList, BitVector.empty))
      }
    }
    "encode" must {
      "encode fixed size of length 4 so that the java avro library can decode it" in {
        val data = hex"00 ff 00 ff".toArray
        val bits = fixedCodec[Byte](size = 4)(byte).encode(data).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        val bytes = Array.fill(4)(0.toByte)
        decoder.readFixed(bytes, 0, 4)

        bytes mustEqual data
      }
    }
  }

  "encode" must {
    "fail if there is not enough input" in {
      val bits = BitVector.fromValidBin("0000")

      fixedCodec[Byte](size = 4)(byte).decode(bits) mustEqual
        failure(Err.insufficientBits(8, 4).pushContext("0"))
    }
    "encode byte array" in {
      val data = hex"00 ff 00 ff".toArray

      fixedCodec[Byte](size = 4)(byte).encode(data).require mustEqual BitVector(data)
    }
  }
  "decode" must {
    "decode byte array 00 ff 00 ff" in {
      val data = hex"00 ff 00 ff".toArray

      fixedCodec[Byte](size = 4)(byte).decode(BitVector(data)) mustEqual
        successful(DecodeResult(data.toList, BitVector.empty))
    }
  }
}
