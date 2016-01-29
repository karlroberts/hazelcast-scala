package com.hazelcast.Scala.aggr

import com.hazelcast.Scala._
import com.hazelcast.Scala.dds.DDS
import com.hazelcast.core.IMap

private[Scala] class SaveToMap[K, V, T](key: K, saveTo: IMap[K, V], val aggr: Aggregator[T, _] { type W = V }) extends Aggregator[T, K] {
  type Q = aggr.Q
  type W = Unit

  def remoteInit: Q = aggr.remoteInit
  def remoteFold(q: Q, t: T): Q = aggr.remoteFold(q, t)
  def remoteCombine(x: Q, y: Q): Q = aggr.remoteCombine(x, y)
  def remoteFinalize(q: Q): Unit = {
    val value = aggr.remoteFinalize(q)
    saveTo.upsert(key, value)(aggr.localCombine(_, value))
  }

  def localCombine(x: Unit, y: Unit): Unit = ()
  def localFinalize(w: Unit) = key

}
