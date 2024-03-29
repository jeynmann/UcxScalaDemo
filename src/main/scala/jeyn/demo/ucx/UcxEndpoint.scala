package jeyn.demo.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants.STATUS
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException}

import java.io.Closeable
import java.nio.ByteBuffer
import java.net.InetSocketAddress
import java.util.concurrent.{CountDownLatch, Future, FutureTask}

import scala.collection.concurrent.TrieMap

trait UcxHandler {
  def onReceive(endpoint: UcxEndpoint, msg: ByteBuffer): Unit = {}
}

class UcxListener(worker: UcxWorker) extends Closeable with Logging {
  private[ucx] val endpoints = new TrieMap[InetSocketAddress, UcxEndpoint]
  private[ucx] var bindFuture: Future[UcpListener] = _
  private[ucx] var handler: UcxHandler = _

  def bind(port: Int): Unit = {
    bindFuture = worker.bind(new InetSocketAddress("0.0.0.0", port), newEndpoint _)
  }

  def bind(address: InetSocketAddress): Unit = {
    bindFuture = worker.bind(address, newEndpoint _)
  }

  def setHandler(h: UcxHandler): Unit = {
    if (!endpoints.isEmpty) {
      endpoints.values.foreach(_.setHandler(h))
    }
    handler = h
  }

  private def newEndpoint(ucxEp: UcxEp, ucxReq: UcxReq): UcxEndpoint = {
    endpoints.getOrElseUpdate(ucxReq.address, {
      val endpoint = new UcxEndpoint(worker)
      if (handler != null) {
        endpoint.setHandler(handler)
      }
      endpoint.connectingCb(ucxEp, ucxReq)
      endpoint.connectFuture = new FutureTask(() => ucxReq)
      endpoint
    })
  }

  override def close() = {
    // TODO
  }
}

class UcxEndpoint(worker: UcxWorker) extends Closeable with Logging {
  private[ucx] var connectFuture: Future[UcxReq] = _
  private[ucx] var remote: InetSocketAddress = _
  private[ucx] var ucxEp: UcxEp = _
  private[ucx] var ucxRecv: UcxRecv = _
  private var bOpened = false
  private var bConnected = false

  def connect(address: InetSocketAddress): Unit = {
    connectFuture = worker.connect(address, connectingCb _)
    remote = address
    bOpened = true
  }

  def send(msg: ByteBuffer): Future[UcxReq] = {
    val hdr = ByteBuffer.allocateDirect(Utils.LONG_SIZE + Utils.LONG_SIZE)
    val hdrPtr = BufferUtils.address(hdr)
    val msgPtr = BufferUtils.address(msg)

    awaitReady()

    hdr.putLong(ucxEp.txId)
    hdr.putLong(0) // TODO: msgId
    hdr.rewind()

    logDebug(s"$ucxEp sending ${remote}: $msg")

    val f = new FutureTask(() => {
      val req = ucxEp.endpoint.sendAmNonBlocking(
        UcxAmID.MESSAGE, hdrPtr, hdr.remaining(), msgPtr, msg.remaining(), 0,
        new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logDebug(s"$ucxEp send ${remote} success: $msg")
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"$ucxEp send ${remote} failed: $errorMsg")
              hdr.clear()
            }
        }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      UcxReq(remote, ucxEp.txId, req)
    })
    worker.post(f)
    f
  }

  def setHandler(handler: UcxHandler): Unit = {
    ucxRecv = new UcxRecv {
      override def onReceive(msg: ByteBuffer) = handler.onReceive(UcxEndpoint.this, msg)
    }
    if (ucxEp != null) {
      worker.setHandler(ucxEp, ucxRecv)
    }
  }

  private[ucx] def awaitReady(): Unit = {
    if (ucxEp == null) {
      assert(connectFuture != null)
      // send connect
      connectFuture.get()
      // wait relpy
      worker.submit(() => worker.progress(() => worker.isConnected(remote))).get()
      // get ep
      ucxEp = worker.getUcxEp(remote)
    }
  }

  private[ucx] def connectingCb(ucxEp: UcxEp, ucxReq: UcxReq): Unit = {
    worker.setHandler(ucxEp, connectedCb _, closedCb _)
  }

  private[ucx] def connectedCb(ucxEp: UcxEp): Unit = {
    if (ucxRecv != null) {
      worker.setHandler(ucxEp, ucxRecv)
    }
    bConnected = true
  }

  private[ucx] def closedCb(ucxEp: UcxEp): Unit = {
    if (ucxRecv != null) {
      worker.setHandler(ucxEp, ucxRecv)
    }
    bConnected = false
  }

  override def close() = {
    // TODO
  }
}
