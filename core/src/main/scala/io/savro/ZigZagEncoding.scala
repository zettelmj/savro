package io.savro

object ZigZagEncoding {
  def encode(i: Int): Int = {
    (i << 1) ^ (i >> 31)
  }

  def decode(i: Int): Int = {
    (i >>> 1) ^ -(i & 1)
  }

  def encode(i: Long): Long = {
    (i << 1) ^ (i >> 63)
  }

  def decode(i: Long): Long = {
    (i >>> 1) ^ -(i & 1)
  }
}
