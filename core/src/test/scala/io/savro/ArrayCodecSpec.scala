package io.savro

import io.savro.AvroTypeCodecs.{arrayCodec, booleanCodec}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.Attempt.successful
import scodec.DecodeResult
import scodec.bits.{BitVector, HexStringSyntax}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class ArrayCodecSpec extends AnyWordSpec with Matchers {
  "interoperability" when {
    "decode" must {
      "be able to decode java avro encoded Array[Boolean]" in {
        val output = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, None.orNull)

        encoder.writeArrayStart()
        encoder.setItemCount(1)
        encoder.startItem()
        encoder.writeBoolean(true)
        encoder.setItemCount(1)
        encoder.startItem()
        encoder.writeBoolean(false)
        encoder.writeArrayEnd()

        encoder.flush()

        val bits = BitVector(output.toByteArray)
        arrayCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Seq(true, false), BitVector.empty))
      }
    }
    "encode" must {
      "be able to encode an Seq[Boolean] so that java avro can read it" in {
        val bits = arrayCodec[Boolean].encode(Seq(false, true)).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readArrayStart() mustEqual 2
        decoder.readBoolean() mustEqual false
        decoder.readBoolean() mustEqual true
      }
      "be able to encode an Seq[Boolean] into multiple blocks so that java avro can read it" in {
        val bits = arrayCodec[Boolean](itemsPerBlock = 1).encode(Seq(false, true)).require

        val input = new ByteArrayInputStream(bits.toByteArray)
        val decoder = DecoderFactory.get().binaryDecoder(input, None.orNull)

        decoder.readArrayStart() mustEqual 1
        decoder.readBoolean() mustEqual false
        decoder.readArrayStart() mustEqual 1
        decoder.readBoolean() mustEqual true
      }
    }
  }

  "decode" must {
    "return an empty Sequence for 00 00" in {
      val bits = hex"00 00".toBitVector

      arrayCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Seq.empty, BitVector.empty))
    }
    "return an array with two items for the single block sequence 04 00 01 00" in {
      val bits = hex"04 00 01 00".toBitVector

      arrayCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Seq(false, true), BitVector.empty))
    }
    "return an array with two items for the multiple block sequence 02 00 02 01 00" in {
      val bits = hex"02 00 02 01 00".toBitVector

      arrayCodec[Boolean].decode(bits) mustEqual successful(DecodeResult(Seq(false, true), BitVector.empty))
    }
  }

  "encode" must {
    "return 00 00 for a empty sequence" in {
      val expectedBits = hex"00 00".toBitVector
      val bits = arrayCodec[Boolean].encode(Seq.empty).require

      bits mustEqual expectedBits
    }
    "return 04 00 01 00 for a sequence with two items" in {
      val expectedBits = hex"04 00 01 00".toBitVector
      val bits = arrayCodec[Boolean].encode(Seq(false, true)).require

      bits mustEqual expectedBits
    }
    "return 02 00 02 01 00 for a sequence with two items and itemsPerBlock set to one" in {
      val bits = arrayCodec[Boolean](itemsPerBlock = 1).encode(Seq(false, true)).require
      val expectedBits = hex"02 00 02 01 00".toBitVector

      bits mustEqual expectedBits
    }
  }
}
