package jeyn.demo.ucx

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants.STATUS
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException}

import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Future, FutureTask}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

trait UcxHandler {
  def onReceive(endpoint: UcxEndpoint, msg: ByteBuffer): Unit = {}
}

class UcxListener(worker: UcxWorker) extends Closeable with UcxLogging {
  private[ucx] val endpoints = new TrieMap[InetSocketAddress, UcxEndpoint]
  private[ucx] var listener: UcpListener = _
  private[ucx] var handler: UcxHandler = _

  def bind(port: Int): Unit = {
    listener = worker.bind(new InetSocketAddress("0.0.0.0", port), newEndpoint _)
  }

  def bind(address: InetSocketAddress): Unit = {
    listener = worker.bind(address, newEndpoint _)
  }

  def newEndpoint(ucxReq: UcxReq): Unit = {
    endpoints.getOrElseUpdate(ucxReq.address, {
      val endpoint = new UcxEndpoint(worker)
      if (handler != null) {
        endpoint.setHandler(handler)
      }
      endpoint.ucxReq = ucxReq
      endpoint
    })
  }

  def setHandler(h: UcxHandler): Unit = {
    if (!endpoints.isEmpty) {
      endpoints.values.foreach(_.setHandler(h))
    }
    handler = h
  }

  override def close() = {
    // TODO
  }
}

class UcxEndpoint(worker: UcxWorker) extends Closeable with UcxLogging {
  private[ucx] var ucxEp: UcxEp = _
  private[ucx] var ucxReq: UcxReq = _
  private[ucx] var ucxRecv: UcxRecv = _

  def connect(address: InetSocketAddress): Unit = {
    ucxReq = worker.connect(address)
  }

  def send(msg: ByteBuffer): UcxReq = {
    val hdr = ByteBuffer.allocateDirect(Utils.LONG_SIZE + Utils.LONG_SIZE)
    val hdrPtr = BufferUtils.address(hdr)
    val msgPtr = BufferUtils.address(msg)

    awaitReady()

    hdr.putLong(ucxEp.txId)
    hdr.putLong(0) // TODO: msgId
    hdr.rewind()

    logDebug(s"$ucxEp sending ${ucxReq.address}: $msg")

    val req = ucxEp.endpoint.sendAmNonBlocking(
      UcxAmID.MESSAGE, hdrPtr, hdr.remaining(), msgPtr, msg.remaining(), 0,
      new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            logDebug(s"$ucxEp send ${ucxReq.address} success: $msg")
          }
          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            logError(s"$ucxEp send ${ucxReq.address} failed: $errorMsg")
            hdr.clear()
          }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    UcxReq(ucxReq.address, ucxEp.txId, req)
  }

  def setHandler(handler: UcxHandler): Unit = {
    ucxRecv = new UcxRecv {
      override def onReceive(msg: ByteBuffer) = handler.onReceive(UcxEndpoint.this, msg)
    }
    if (ucxEp != null) {
      worker.setHandler(ucxEp, ucxRecv)
    }
  }

  private def awaitReady(): Unit = {
    if (ucxEp == null) {
      assert(ucxReq != null)

      val latch = new CountDownLatch(1)
      worker.submit(() => {
        worker.progress(() => worker.isConnected(ucxReq.address))
        latch.countDown()
      })
      latch.await()

      ucxEp = worker.getUcxEp(ucxReq.address)
      if (ucxRecv != null) {
        worker.setHandler(ucxEp, ucxRecv)
      }
    }
  }

  override def close() = {
    // TODO
  }
}

class UcxWorker(val worker: UcpWorker, id: Long = 0) extends Closeable with UcxLogging {
  // private val hostName = InetAddress.getLocalHost.getHostName
  // private val uniName = s"${ManagementFactory.getRuntimeMXBean.getName}#$id"
  // private val uniId: Long = wroker.getNativeId()

  private val executor = new WorkerThread(worker, true)

  private val rxHandlers = new TrieMap[Long, UcxRecv]
  private val connectedSAs = new mutable.HashMap[InetSocketAddress, UcxEp]
  private val connectedEps = new mutable.HashMap[UcpEndpoint, UcxSA]
  private val connectingEps = new mutable.HashMap[UcpEndpoint, UcxReq]
  private var listeners = new mutable.HashMap[InetSocketAddress, UcpListener]

  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ep: UcpEndpoint, ecode: Int, err: String): Unit = {
      connectedEps.remove(ep).map(ucxEp => {
        val address = ucxEp.address
        if (ecode == STATUS.UCS_ERR_CONNECTION_RESET) {
          logInfo(s"Connection to $address closed.")
        } else {
          logWarning(s"Connection to $address error: $err")
        }
        connectedSAs.remove(address)
        rxHandlers.remove(ucxEp.rxId).map(handler =>
            logInfo(s"Remove $handler of $address."))
        ep.close()
      })
    }
  }

  worker.setAmRecvHandler(UcxAmID.CONNECT,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) => {
    val header = BufferUtils.makeByteBuffer(headerAddress, headerSize.toInt)
    val rxId = ep.getNativeId()
    val txId = header.getLong()

    handleConnect(ep, txId, rxId)
    STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  worker.setAmRecvHandler(UcxAmID.CONNECT_REPLY,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) => {
    val header = BufferUtils.makeByteBuffer(headerAddress, headerSize.toInt)
    val rxId = ep.getNativeId()
    val txId = header.getLong()

    handleConnectReply(ep, txId, rxId)
    STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  worker.setAmRecvHandler(UcxAmID.MESSAGE,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = BufferUtils.makeByteBuffer(headerAddress, headerSize.toInt)
    val rxId = header.getLong()
    val msgId = header.getLong()

    handleMessage(rxId, msgId, amData)
    STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  def start(): Unit = {
    executor.start()
  }

  def closing(): Future[Unit.type] = {
    val cleanTask = new FutureTask(new Runnable {
      override def run() = close()
    }, Unit)
    executor.close(cleanTask)
    cleanTask
  }

  def newListener(): UcxListener = {
    new UcxListener(this)
  }

  def newEndpoint(): UcxEndpoint = {
    new UcxEndpoint(this)
  }

  override def close(): Unit = {
    if (!connectedEps.isEmpty) {
      logInfo(s"$id closing ${connectedSAs.size} clients")
      connectedEps.keys.map(
        _.closeNonBlockingFlush()).foreach(progress(_))
      connectedEps.clear()
      connectedSAs.clear()
      rxHandlers.clear()
    }
    if (!listeners.isEmpty) {
      listeners.values.foreach(_.close())
      listeners.clear()
    }
  }

  private[ucx] def bind(bindSA: InetSocketAddress, connectingCb: UcxReq => Unit): UcpListener = {
    val handler = new UcpListenerConnectionHandler {
      override def onConnectionRequest(conReq: UcpConnectionRequest): Unit = {
        val address = conReq.getClientAddress()
        val id = conReq.getClientId()
        val params = new UcpEndpointParams().setConnectionRequest(conReq)
            .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
            .setName(s"$bindSA receive connect from $address-$id")
        val ep = worker.newEndpoint(params)
        val ucxReq = UcxReq(address, 0, null)
        connectingEps.getOrElseUpdate(ep, ucxReq)
        connectingCb(ucxReq)
      }
    }
    val listenerParams = new UcpListenerParams().setSockAddr(bindSA)
        .setConnectionHandler(handler)

    listeners.getOrElseUpdate(bindSA, { worker.newListener(listenerParams) })
  }

  private[ucx] def connect(server: InetSocketAddress): UcxReq = {
    val params = new UcpEndpointParams().setSocketAddress(server)
      .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
      .setName(s"Client to $server").sendClientId()

    val header = ByteBuffer.allocateDirect(Utils.LONG_SIZE)
    val ptr = BufferUtils.address(header)

    logDebug(s"$id connecting to $server")

    val ep = worker.newEndpoint(params)
    val rxId = ep.getNativeId()
    header.putLong(rxId)
    header.rewind()

    val req = ep.sendAmNonBlocking(UcxAmID.CONNECT, ptr, header.remaining(), ptr, 0,
        UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
        new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logDebug(s"$id CONNECT to $server success")
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"$id CONNECT to $server failed: $errorMsg")
              header.clear()
            }
        }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    connectingEps.getOrElseUpdate(ep, UcxReq(server, 0, req))
  }

  private[ucx] def submit(task: Runnable): Unit = {
    executor.post(task)
  }

  private[ucx] def progress(req: UcpRequest): Unit = {
    while (!req.isCompleted) {
      worker.progress()
    }
  }

  private[ucx] def progress(done: () => Boolean): Unit = {
    while (!done()) {
      worker.progress()
    }
  }

  private[ucx] def getUcxEp(address: InetSocketAddress): UcxEp = {
    connectedSAs(address)
  }

  private[ucx] def isConnected(address: InetSocketAddress): Boolean = {
    connectedSAs.contains(address)
  }

  private[ucx] def getUcxEp(ep: UcpEndpoint): UcxSA = {
    connectedEps(ep)
  }

  private[ucx] def isConnected(ep: UcpEndpoint): Boolean = {
    connectedEps.contains(ep)
  }

  private[ucx] def setHandler(ucxEp: UcxEp, handler: UcxRecv): Option[UcxRecv] = {
    rxHandlers.put(ucxEp.rxId, handler)
  }

  private def connected(ep: UcpEndpoint, txId: Long, rxId: Long): Unit = {
    connectingEps.remove(ep).map(req => {
      connectedEps.getOrElseUpdate(ep, UcxSA(req.address, txId, rxId))
      connectedSAs.getOrElseUpdate(req.address, UcxEp(ep, txId, rxId))
    })
  }

  private def handleConnect(ep: UcpEndpoint, txId: Long, rxId: Long): Unit = {
    val header = ByteBuffer.allocateDirect(Utils.LONG_SIZE)
    val ptr = BufferUtils.address(header)

    header.putLong(rxId)
    header.rewind()

    executor.post(() => {
      ep.sendAmNonBlocking(UcxAmID.CONNECT_REPLY, ptr, header.remaining(), ptr, 0,
          UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY,
          new UcxCallback {
              override def onSuccess(request: UcpRequest): Unit = {
                connected(ep, txId, rxId)
                logDebug(s"$id CONNECT_REPLY to $ep success")
              }
              override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                header.clear()
                logError(s"$id CONNECT_REPLY to $ep failed: $errorMsg")
              }
          }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      })
  }

  private def handleConnectReply(ep: UcpEndpoint, txId: Long, rxId: Long): Unit = { 
    connected(ep, txId, rxId)
  }

  private def handleMessage(rxId: Long, msgId: Long, amData: UcpAmData): Unit = {
    if (amData.isDataValid) {
      val amBuf = BufferUtils.makeByteBuffer(amData.getDataAddress, amData.getLength.toInt)
      rxHandlers.get(rxId).map(_.onReceive(amBuf))
      logDebug(s"${id}-${rxId} recv success: $amData")
    } else {
      val recvBuf = ByteBuffer.allocateDirect(amData.getLength.toInt)
      val recvPtr = BufferUtils.address(recvBuf)
      worker.recvAmDataNonBlocking(
        amData.getDataHandle, recvPtr, recvBuf.remaining(),
        new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              rxHandlers.get(rxId).map(_.onReceive(recvBuf))
              logDebug(s"${id}-${rxId} recv success: $request")
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"${id}-${rxId} recv failed: $errorMsg")
            }
        }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }
}

object UcxWorker {
    private val localWorker = new ThreadLocal[UcxWorker]
    private def set(worker: UcxWorker) = {
        localWorker.set(worker)
    }
    def get: UcxWorker = localWorker.get
}

private[ucx] case class UcxEp(endpoint: UcpEndpoint, txId: Long, rxId: Long) {}
private[ucx] case class UcxSA(address: InetSocketAddress, txId: Long, rxId: Long) {}
private[ucx] case class UcxReq(address: InetSocketAddress, txId: Long, req: UcpRequest) {}

private[ucx] trait UcxRecv {
  def onReceive(msg: ByteBuffer): Unit = {}
}

private[ucx] object UcxAmID {
    val CONNECT = 0
    val CONNECT_REPLY = 1
    val MESSAGE = 10
}
