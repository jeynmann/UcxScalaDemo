package org.openucx.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.{AbstractExecutorService, TimeUnit, Callable}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Set

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE

import org.apache.logging.log4j.LogManager;


class UcxWorkerService extends AbstractExecutorService {
    val id = new AtomicInteger()
    private val ucpWorkerParams = new UcpWorkerParams()
            .requestThreadSafety()
            .requestWakeupRX()
            .requestWakeupTX()
            .requestWakeupEdge()
    private var executors: Array[UcxWorkerWrapper] = _
    private var listener: UcxWorkerWrapper = _
    private var ucpListener: UcpListener = _
    private var connectionHandler: UcpListenerConnectionHandler = _
    private var connectbackHandler: UcpAmRecvCallback = _
    private var bShutDown = false
    private var bTermed = false
    private var bInit = false

    private final val connections = new TrieMap[String, UcpEndpoint]
    
    def initServer(n: Int, hostPort: String, handles: Array[(Int, UcpAmRecvCallback, Long)] = null) = {
        initExecutors(n, null, 32)
        initListener(hostPort, handles)
        bInit = true
    }

    def initClient(n: Int, hostPortList: String = "", handles: Array[(Int, UcpAmRecvCallback, Long)] = null, numTasks: Int = 0) = {
        initExecutors(n, handles)
        if (numTasks > 0) {
            executors.foreach(_.initTaskLimit(numTasks))
        }
        if (!hostPortList.isEmpty) {
            for (hostPort <- hostPortList.split(",")) {
                UcxWorkerService.serverSocket.getOrElseUpdate(hostPort, {
                    val host = hostPort.split(":")
                    new InetSocketAddress(host(0), host(1).toInt)
                })
            }
            executors.foreach { (x) => {
                x.submit(newTaskFor(new Runnable {
                    override def run = {
                        UcxWorkerService.serverSocket.keys.foreach(x.getOrConnect(_))
                    }
                }, Unit))
            }}
            // for (hostPort <- hostPortList.split(",")) {
            //     UcxWorkerService.serverSocket.getOrElseUpdate(hostPort, {
            //         val host = hostPort.split(":")
            //         new InetSocketAddress(host(0), host(1).toInt)
            //     })
            //     executors.foreach(_.getOrConnect(hostPort))
            // }
        }
        bInit = true
    }

    private def initExecutors(n: Int, handles: Array[(Int, UcpAmRecvCallback, Long)], shift: Int = 0) = {
        executors = new Array[UcxWorkerWrapper](n)
        for (i <- 0 until n) {
            val id = (i + 1).toLong << shift
            if (shift == 0) {
                ucpWorkerParams.setClientId(id)
            }
            executors(i) = new UcxWorkerWrapper(UcxWorkerService.ucxContext.newWorker(ucpWorkerParams), id)
            executors(i).initService(handles)
            executors(i).start
        }
    }

    private def initListener(hostPort: String, handles: Array[(Int, UcpAmRecvCallback, Long)]) = {
        val listenAddress = UcxWorkerService.listenSocket.getOrElseUpdate(hostPort, {
            if (hostPort.contains(":")) {
                val host = hostPort.split(":")
                new InetSocketAddress(host(0), host(1).toInt)
            } else {
                new InetSocketAddress("0.0.0.0", hostPort.toInt)
            }
        })
        Log.debug(s"listener on ${listenAddress}")
        listener = new UcxWorkerWrapper(UcxWorkerService.ucxContext.newWorker(ucpWorkerParams))
        // message handles
        listener.initService(handles)
        connectionHandler = new UcpListenerConnectionHandler {
            override def onConnectionRequest(req: UcpConnectionRequest) = {
                Log.debug(s"Listener $this receive connecting from ${req.getClientId}")

                val ep = listener.worker.newEndpoint(
                    new UcpEndpointParams()
                        .setConnectionRequest(req)
                        .setPeerErrorHandlingMode()
                        .setErrorHandler(new UcpEndpointErrorHandler {
                            override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
                                if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                                    Log.warn(s"Connection closed on ep: $ucpEndpoint")
                                } else {
                                    Log.error(s"Ep $ucpEndpoint got an error: $errorString")
                                }
                                UcxWorkerService.endpoints.remove(ucpEndpoint)
                                ucpEndpoint.close()
                            }
                        })
                        .setName(s"Endpoint to ${req.getClientId}")
                )
                UcxWorkerService.endpoints.add(ep)
            }
        }
        connectbackHandler = new UcpAmRecvCallback {
            override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) = {
                val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
                val workerName = java.nio.charset.StandardCharsets.UTF_8.decode(header).toString

                UcxWorkerService.clientWorker.put(workerName, workerAddress)
                executors.foreach { x =>
                    x.submit(newTaskFor(new Runnable {
                        override def run = x.getOrConnectBack(workerName)
                    }, Unit))
                }
                // executors.foreach {
                //     _.getOrConnectBack(workerName)
                // }
                UcsConstants.STATUS.UCS_OK
            }
        }
        listener.worker.setAmRecvHandler(
            UcxAmId.CONNECTION.id, connectbackHandler, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
        ucpListener = listener.worker.newListener(new UcpListenerParams()
            .setSockAddr(listenAddress)
            .setConnectionHandler(connectionHandler))
        listener.start
    }

    @inline
    def selectRR = {
        val i = (id.getAndIncrement % executors.size).abs
        executors(i)
    }

    override def execute(task: Runnable) = submit(task)

    def submit(task: Callable[_]) = {
        val f = newTaskFor(task)
        selectRR.submit(f)
        f
    }

    override def submit(task: Runnable) = {
        val f = newTaskFor(task, Unit)
        selectRR.submit(f)
        f
    }

    override def submit[T](task: Runnable, result: T) = {
        val f = newTaskFor(task, result)
        selectRR.submit(f)
        f
    }

    def run = {}

    def close(timeout: Long = 0L, unit: TimeUnit = TimeUnit.MILLISECONDS) = {
        executors.foreach (x => {
            x.interrupt()
            x.join(unit.toMillis(timeout))
            x.close()
        })
        bShutDown = true
        bTermed = true
    }
    override def isShutdown() = bShutDown
    override def isTerminated() = bTermed
    override def awaitTermination(timeout: Long, unit: TimeUnit) = {
        close(timeout, unit)
        true
    }
    override def shutdown() = close()
    override def shutdownNow(): java.util.List[Runnable] = {
        close()
        null
    } 
}

object UcxWorkerService {
    val ucpParams = new UcpParams()
        .requestAmFeature()
        .setMtWorkersShared(true)
        .setConfig("USE_MT_MUTEX", "yes")
        .requestWakeupFeature()
        
    val ucxContext = new UcpContext(ucpParams)
    val listenSocket = new TrieMap[String, InetSocketAddress]
    val serverSocket = new TrieMap[String, InetSocketAddress]
    val clientWorker = new TrieMap[String, ByteBuffer]
    val endpoints = Set.empty[UcpEndpoint]
}

object UcxAmId extends Enumeration { 
    val CONNECTION = Value
    val INTRODUCE = Value
    val FETCH = Value
    val FETCH_REPLY = Value
}

object Log {
    val log = LogManager.getLogger(UcxWorkerService.getClass)
    def error(buf: String) = log.error(buf)
    def info(buf: String) = log.info(buf)
    def warn(buf: String) = log.warn(buf)
    def debug(buf: String) = log.debug(buf)
    def trace(buf: String) = log.trace(buf)
}