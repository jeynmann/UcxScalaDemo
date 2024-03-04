package jeyn.demo.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{AbstractExecutorService, TimeUnit, Callable}
import java.util.concurrent.atomic.AtomicInteger

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Set

class Transport extends UcxLogging {
  val ucpParams = new UcpParams()
    .requestAmFeature()
    .setMtWorkersShared(true)
    .setConfig("USE_MT_MUTEX", "yes")
    .requestWakeupFeature()
      
  val ucpWorkerParams = new UcpWorkerParams()
    .requestThreadSafety()
    .requestWakeupRX()
    .requestWakeupTX()
    .requestWakeupEdge()

  val ucxContext = new UcpContext(ucpParams)
  val ucxWorker = new UcxWorker(ucxContext.newWorker(ucpWorkerParams), 0)
  var ucxListerner: UcxListener = _
  var ucxEndpoint: UcxEndpoint = _
}
