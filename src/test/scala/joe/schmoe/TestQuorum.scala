package joe.schmoe

import org.junit._
import org.junit.Assert._
import com.hazelcast.Scala._
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicInteger
import com.hazelcast.core.IMap
import com.hazelcast.map.impl.MapService
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.quorum.QuorumFunction
import com.hazelcast.core.HazelcastInstanceAware
import java.util.Collection
import com.hazelcast.core.Member

object TestQuorum extends ClusterSetup {
  override def clusterSize = 1
  @volatile var quorumHz: HazelcastInstance = _
  def init = {
    val func = new QuorumFunction with HazelcastInstanceAware {
      def setHazelcastInstance(hz: HazelcastInstance) = {
        quorumHz = hz
      }
      def apply(members: Collection[Member]) = true
    }
    memberConfig.getQuorumConfig("default") setQuorumFunctionImplementation func
  }
  def destroy = ()
}

class TestQuorum {
  import TestQuorum._

  @Test
  def justChecking {
    val quorum = hz(0).getQuorumService.getQuorum("default")
    assertTrue(quorum.isPresent)
    assertNotNull(quorumHz)
  }
}
