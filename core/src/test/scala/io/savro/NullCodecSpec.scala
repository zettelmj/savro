package io.savro

import io.savro.AvroTypeCodecs.nullCodec
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.DecodeResult
import scodec.bits.BitVector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class NullCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" must {
    "be able to decode java avro encoded byte array" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeNull()
      encoder.flush()
      val bit = BitVector(output.toByteArray)

      nullCodec.decode(bit) mustEqual successful(DecodeResult((), BitVector.empty))
    }
    "avro java encoded byte array must be decodable by java avro" in {
      val bits = nullCodec.encode(List.fill(2)(64.toByte)).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readNull()
    }
  }

  "null codec" must {
    "succesfully decode eight zero bits" in {
      val bits = BitVector.empty
      nullCodec.decode(bits) mustBe successful(DecodeResult((), BitVector.empty))
    }
    "encode Unit" in {
      nullCodec.encode(()) mustBe successful(BitVector.empty)
    }
  }
}
