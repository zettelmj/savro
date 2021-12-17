package io.savro

import io.savro.AvroTypeCodecs.{booleanCodec, longCodec, mapCodec}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.{failure, successful}
import scodec.bits.{BitVector, HexStringSyntax}
import scodec.{DecodeResult, Err}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class MapCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" when {
    "decode" must {
      "be able to decode java avro encoded Map[String, Long]" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeMapStart()
        encoder.setItemCount(2)
        encoder.startItem()
        encoder.writeString("item1")
        encoder.writeLong(1)
        encoder.writeString("item2")
        encoder.writeLong(2)
        encoder.writeMapEnd()

        encoder.flush()
        val bit = BitVector(output.toByteArray)
        mapCodec[Long].decode(bit) mustEqual
          successful(DecodeResult(Map("item1" -> 1, "item2" -> 2), BitVector.empty))
      }
      "be able to decode java avro encoded Map[String, Boolean] with with more than one block" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeMapStart()
        encoder.setItemCount(2)
        encoder.startItem()
        encoder.writeString("a")
        encoder.writeBoolean(true)
        encoder.writeString("b")
        encoder.writeBoolean(false)
        encoder.setItemCount(2)
        encoder.startItem()
        encoder.writeString("c")
        encoder.writeBoolean(true)
        encoder.writeString("d")
        encoder.writeBoolean(false)
        encoder.writeMapEnd()

        encoder.flush()
        val bit = BitVector(output.toByteArray)
        mapCodec[Boolean].decode(bit) mustEqual
          successful(DecodeResult(Map("a" -> true, "b" -> false, "c" -> true, "d" -> false), BitVector.empty))
      }
    }
    "encode" must {
      "avro encoded Map[String, Long] must be decodable by java avro" in {
        val bits = mapCodec[Long].encode(Map("item1" -> 1, "item2" -> 2)).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readMapStart() mustEqual 2
        decoder.readString() mustEqual "item1"
        decoder.readLong() mustEqual 1
        decoder.readString() mustEqual "item2"
        decoder.readLong() mustEqual 2
        decoder.mapNext() mustEqual 0
      }
    }
  }

  "encode" must {
    "return 00 for an empty map" in {
      val expectedBits = hex"00 00".toBitVector
      mapCodec[Boolean].encode(Map()) mustEqual successful(expectedBits)
    }
    "return a for a map of boolean" in {
      val expectedBits = hex"04 02 61 01 02 62 00 00".toBitVector
      mapCodec[Boolean](booleanCodec).encode(Map("a" -> true, "b" -> false)) mustEqual successful(expectedBits)
    }
    "encode a map with four entries into two blocks if itemsPerBlock is set to two" in {
      val expectedBits = hex"04 02 61 01 02 62 00 04 02 63 01 02 64 00 00".toBitVector
      mapCodec[Boolean](itemsPerBlock = 2)(booleanCodec).encode(Map("a" -> true, "b" -> false, "c" -> true, "d" -> false)) mustEqual successful(expectedBits)
    }
  }
  "decode" must {
    "return error if not enough bits are available" in {
      val bits = hex"00".toBitVector
      mapCodec[Boolean].decode(bits) mustEqual failure(Err.insufficientBits(8, 0))
    }
    "return an empty map for the input 00 00" in {
      val bits = hex"00 00".toBitVector
      mapCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Map(), BitVector.empty))
    }
    "return an map with two entries for the input 04 02 61 01 02 62 00 00" in {
      val bits = hex"04 02 61 01 02 62 00 00".toBitVector
      mapCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Map("a" -> true, "b" -> false), BitVector.empty))
    }
    "return a map if more than one block is available" in {
      val bits = hex"04 02 61 01 02 62 00 04 02 63 01 02 64 00 00".toBitVector
      mapCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Map("a" -> true, "b" -> false, "c" -> true, "d" -> false), BitVector.empty))
    }
  }
}
