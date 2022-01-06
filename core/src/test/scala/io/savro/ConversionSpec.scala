package io.savro

import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.DecodeResult
import scodec.bits.{BitVector, HexStringSyntax}
import shapeless.HNil

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class ConversionSpec extends AnyWordSpec with Matchers {

  import io.savro.AvroTypeCodecs._

  private val data = 1 :: "Test" :: HNil
  private val codec = ComplexCodec.apply(data)

  "interoperability" must {
    "convert hlist into avro java readable bit stream" in {
      val bits = codec.encode(data).require

      val input = new ByteArrayInputStream(bits.toByteArray)
      val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

      decoder.readInt() mustEqual 1
      decoder.readString() mustEqual "Test"
    }
    "able to read avro java bit stream into hlist" in {
      val output = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

      encoder.writeInt(1)
      encoder.writeString("Test")
      encoder.flush()

      val bit = BitVector(output.toByteArray)
      codec.decode(bit) mustEqual successful(DecodeResult(1 :: "Test" :: HNil, BitVector.empty))
    }
  }

  "ComplexCodec.apply" must {
    "be able to hlist codec to encode hlist" in {
      codec.encode(data).require mustEqual hex"020854657374".toBitVector
    }
    "be able to use case class codec to decode to case class" in {
      val bits = hex"020854657374".toBitVector

      codec.decode(bits) mustEqual successful(DecodeResult(data, BitVector.empty))
    }
  }
}
