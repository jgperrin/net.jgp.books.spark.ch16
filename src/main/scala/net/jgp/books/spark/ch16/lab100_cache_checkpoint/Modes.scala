package net.jgp.books.spark.ch16.lab100_cache_checkpoint

object Mode extends Enumeration {
  type Mode = Value
  val NoCacheNoCheckPoint, Cache, CheckPoint, CheckPointNonEager = Value
}
