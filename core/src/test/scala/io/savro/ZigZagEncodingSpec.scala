package io.savro

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scodec.bits.{BitVector, HexStringSyntax}


class ZigZagEncodingSpec extends AnyWordSpec with Matchers {

  "ZigZag" must {
    "encode integers" in {
      BitVector.fromInt(ZigZagEncoding.encode(0)).toByteVector mustEqual hex"0x00 00 00 00"
      BitVector.fromInt(ZigZagEncoding.encode(-1)).toByteVector mustEqual hex"0x00 00 00 01"
      BitVector.fromInt(ZigZagEncoding.encode(1)).toByteVector mustEqual hex"0x00 00 00 02"
      BitVector.fromInt(ZigZagEncoding.encode(-2)).toByteVector mustEqual hex"0x00 00 00 03"
      BitVector.fromInt(ZigZagEncoding.encode(2)).toByteVector mustEqual hex"0x00 00 00 04"
      BitVector.fromInt(ZigZagEncoding.encode(Int.MaxValue)).toByteVector mustEqual hex"0xff ff ff fe"
      BitVector.fromInt(ZigZagEncoding.encode(Int.MinValue)).toByteVector mustEqual hex"0xff ff ff ff"
    }
    "encode long" in {
      BitVector.fromLong(ZigZagEncoding.encode(0L)).toByteVector mustEqual hex"0x00 00 00 00 00 00 00 00"
      BitVector.fromLong(ZigZagEncoding.encode(-1L)).toByteVector mustEqual hex"0x00 00 00 00 00 00 00 01"
      BitVector.fromLong(ZigZagEncoding.encode(1L)).toByteVector mustEqual hex"0x00 00 00 00 00 00 00 02"
      BitVector.fromLong(ZigZagEncoding.encode(-2L)).toByteVector mustEqual hex"0x00 00 00 00 00 00 00 03"
      BitVector.fromLong(ZigZagEncoding.encode(2L)).toByteVector mustEqual hex"0x00 00 00 00 00 00 00 04"
      BitVector.fromLong(ZigZagEncoding.encode(Long.MaxValue)).toByteVector mustEqual hex"0xff ff ff ff ff ff ff fe"
      BitVector.fromLong(ZigZagEncoding.encode(Long.MinValue)).toByteVector mustEqual hex"0xff ff ff ff ff ff ff ff"
    }
    "decode integers" in {
      ZigZagEncoding.decode(hex"0x00 00 00 00".toInt()) mustEqual 0
      ZigZagEncoding.decode(hex"0x00 00 00 01".toInt()) mustEqual -1
      ZigZagEncoding.decode(hex"0x00 00 00 02".toInt()) mustEqual 1
      ZigZagEncoding.decode(hex"0x00 00 00 03".toInt()) mustEqual -2
      ZigZagEncoding.decode(hex"0x00 00 00 04".toInt()) mustEqual 2
      ZigZagEncoding.decode(hex"0xff ff ff fe".toInt()) mustEqual Int.MaxValue
      ZigZagEncoding.decode(hex"0xff ff ff ff".toInt()) mustEqual Int.MinValue
    }
    "decide long" in {
      ZigZagEncoding.decode(hex"0x00 00 00 00 00 00 00 00".toLong()) mustEqual 0
      ZigZagEncoding.decode(hex"0x00 00 00 00 00 00 00 01".toLong()) mustEqual -1
      ZigZagEncoding.decode(hex"0x00 00 00 00 00 00 00 02".toLong()) mustEqual 1
      ZigZagEncoding.decode(hex"0x00 00 00 00 00 00 00 03".toLong()) mustEqual -2
      ZigZagEncoding.decode(hex"0x00 00 00 00 00 00 00 04".toLong()) mustEqual 2
      ZigZagEncoding.decode(hex"0xff ff ff ff ff ff ff fe".toLong()) mustEqual Long.MaxValue
      ZigZagEncoding.decode(hex"0xff ff ff ff ff ff ff ff".toLong()) mustEqual Long.MinValue
    }
  }
}
