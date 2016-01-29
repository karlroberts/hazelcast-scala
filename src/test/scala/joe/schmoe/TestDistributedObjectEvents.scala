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

object TestDistributedObjectEvents extends ClusterSetup {
  override def clusterSize = 1
  def init = ()
  def destroy = ()
}

class TestDistributedObjectEvents {
  import TestDistributedObjectEvents._

  @Test
  def any {
    any(client)
    any(hz(0))
  }
  private def any(hz: HazelcastInstance) {
    val expected = 2 // 1 created, 1 destroyed
    val counter = new AtomicInteger
    val cdl = new CountDownLatch(expected)
    val reg = hz.onDistributedObjectEvent {
      case DistributedObjectCreated(name, disobj) =>
        counter.incrementAndGet()
        cdl.countDown()
      case DistributedObjectDestroyed(name, svcName) =>
        counter.incrementAndGet()
        cdl.countDown()
    }
    val anyMap = hz.getMap[Int, Int]("anyMap")
    anyMap.put(1, 1)
    anyMap.destroy()
    assertTrue(cdl.await(5, SECONDS))
    assertEquals(expected, counter.get)
    reg.cancel()
  }

  @Test
  def typed {
    typed(client)
    typed(hz(0))
  }
  private def typed(hz: HazelcastInstance) {
    val expected = 4 // 2 IMap created, 2 IMap destroyed
    val counter = new AtomicInteger
    val cdl = new CountDownLatch(expected)
    val reg = hz.onDistributedObjectEvent {
      case DistributedObjectCreated(_, imap: IMap[_, _]) =>
        counter.incrementAndGet()
        cdl.countDown()
      case DistributedObjectDestroyed(_, MapService.SERVICE_NAME) =>
        counter.incrementAndGet()
        cdl.countDown()
    }
    val fooMap = hz.getMap[Int, Int]("foo")
    fooMap.put(1, 1)
    val fooQueue = hz.getQueue[Int]("foo")
    fooQueue.offer(1)
    val barMap = hz.getMap[Int, Int]("bar")
    barMap.put(1, 1)
    val barbeQueue = hz.getQueue[Int]("bar")
    barbeQueue.offer(1)
    fooMap.destroy()
    fooQueue.destroy()
    barMap.destroy()
    barbeQueue.destroy()
    assertTrue(cdl.await(5, SECONDS))
    assertEquals(expected, counter.get)
    reg.cancel()
  }

  @Test
  def named {
    named(client)
    named(hz(0))
  }
  private def named(hz: HazelcastInstance) {
    val expected = 4 // 2 "foo" created, 2 "foo" destroyed
    val counter = new AtomicInteger
    val cdl = new CountDownLatch(expected)
    val reg = hz.onDistributedObjectEvent {
      case DistributedObjectCreated("foo", _) =>
        counter.incrementAndGet()
        cdl.countDown()
      case DistributedObjectDestroyed("foo", _) =>
        counter.incrementAndGet()
        cdl.countDown()
    }
    val fooMap = hz.getMap[Int, Int]("foo")
    fooMap.put(1, 1)
    val fooQueue = hz.getQueue[Int]("foo")
    fooQueue.offer(1)
    val barMap = hz.getMap[Int, Int]("bar")
    barMap.put(1, 1)
    val barbeQueue = hz.getQueue[Int]("bar")
    barbeQueue.offer(1)
    fooMap.destroy()
    fooQueue.destroy()
    barMap.destroy()
    barbeQueue.destroy()
    assertTrue(cdl.await(5, SECONDS))
    assertEquals(expected, counter.get)
    reg.cancel()
  }

}
