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
    private var connectbackHandler: UcpAmRecvCallback = _
    private var introduceHandler: UcpAmRecvCallback = _
    private var joiningHandler: UcpAmRecvCallback = _
    private var bClient: Boolean = _
    private var bSequentialConnecting = false
    private var bShutDown = false
    private var bTermed = false
    private var bInit = false

    def initServer(n: Int, hostPort: String, handles: Array[(Int, UcpAmRecvCallback, Long)] = null) = {
        bClient = false
        initExecutors(n, null)
        initListener(hostPort, handles)
        bInit = true
    }

    def initClient(n: Int, hostPortList: String = "", handles: Array[(Int, UcpAmRecvCallback, Long)] = null, numTasks: Int = 0) = {
        bClient = true
        initExecutors(n, handles)
        if (numTasks > 0) {
            executors.foreach(_.initTaskLimit(numTasks))
        }
        if (!hostPortList.isEmpty) {
            for (hostPort <- hostPortList.split(",")) {
                if (!hostPort.isEmpty) {
                    UcxWorkerService.serverSocket.getOrElseUpdate(hostPort, {
                        Utils.newInetSocketAddress(hostPort)
                    })
                }
            }
            if (bSequentialConnecting) {
                executors.foreach { x => UcxWorkerService.serverSocket.keys.foreach(x.connecting(_)) }
            } else {
                executors.foreach { x =>
                    x.submit(newTaskFor(new Runnable {
                        override def run = UcxWorkerService.serverSocket.keys.foreach(x.connecting(_))
                    }, Unit))
                }
            }
        }
        bInit = true
    }

    private def initExecutors(n: Int, handles: Array[(Int, UcpAmRecvCallback, Long)]) = {
        val shift = if (bClient) 32 else 0
        executors = new Array[UcxWorkerWrapper](n)
        for (i <- 0 until n) {
            val id = (i + 1).toLong << shift
            if (bClient) {
                ucpWorkerParams.setClientId(id)
            }
            executors(i) = new UcxWorkerWrapper(UcxWorkerService.ucxContext.newWorker(ucpWorkerParams), id)
            executors(i).initService(handles)
        }
    }

    private def initListener(hostPort: String, handles: Array[(Int, UcpAmRecvCallback, Long)]) = {
        val listenAddress = UcxWorkerService.listenSocket.getOrElseUpdate(hostPort, {
            Utils.newInetSocketAddress(hostPort)
        })
        Log.debug(s"listener on ${listenAddress}")

        listener = new UcxWorkerWrapper(UcxWorkerService.ucxContext.newWorker(ucpWorkerParams), 0, hostPort)
        // message handles
        listener.initService(handles)
        connectbackHandler = new UcpAmRecvCallback {
            override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) = {
                val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                val workerName = Utils.newString(header)

                val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)

                UcxWorkerService.clientWorker.put(workerName, Utils.newBuffer(workerAddress))

                if (bSequentialConnecting) {
                    executors.foreach { x => x.getOrConnectBack(workerName) }
                } else {
                    executors.foreach { x =>
                        x.submit(newTaskFor(new Runnable {
                            override def run = x.getOrConnectBack(workerName)
                        }, Unit))
                    }
                }
                // @attention may should be in progress if not copy amData
                UcsConstants.STATUS.UCS_OK
            }
        }
        listener.worker.setAmRecvHandler(
            UcxAmId.CONNECTION.id, connectbackHandler, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
        ucpListener = listener.worker.newListener(new UcpListenerParams()
            .setSockAddr(listenAddress)
            .setConnectionHandler(listener.getConnectionHandle))
    }

    def initCluster(leader: String, clientService: UcxWorkerService): Unit = {
        if (!leader.isEmpty) {
            UcxWorkerService.leaderSocket.getOrElseUpdate(
                leader, Utils.newInetSocketAddress(leader))
        }
        Option(clientService) match {
            case Some(service) => {
                Log.debug(s"launch in cluster mode.")
                initJoiningHandle(service)
                initIntroduceHandle(service)
            }
            case None => ()
        }
    }

    def initJoiningHandle(clientService: UcxWorkerService): Unit = {
        joiningHandler = new UcpAmRecvCallback {
            override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) = {
                val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                val hostName = Utils.newString(header)

                val socketBuffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)

                Log.debug(s"Leader $listener receive joining from ${hostName}")

                UcxWorkerService.serverSocket.getOrElseUpdate(hostName, Utils.newInetSocketAddress4(socketBuffer))

                // connect to new member
                if (bSequentialConnecting) {
                    clientService.executors.foreach { x => x.connecting(hostName) }
                    // new member connect existing members
                    clientService.selectRR.introduceAll(hostName)
                    // existing members connect to new member
                    clientService.selectRR.introduce(hostName, socketBuffer)
                } else {
                    clientService.executors.foreach { x =>
                        x.submit(newTaskFor(new Runnable {
                            override def run = x.connecting(hostName)
                        }, Unit))
                    }
                    clientService.submit(new Runnable {
                        override def run = UcxWorkerWrapper.get.introduceAll(hostName)
                    })
                    clientService.submit(new Runnable {
                        val copiedBuffer = Utils.newBuffer(socketBuffer)
                        override def run = UcxWorkerWrapper.get.introduce(hostName, copiedBuffer)
                    })
                }

                // done
                UcsConstants.STATUS.UCS_OK
            }
        }
        listener.worker.setAmRecvHandler(
            UcxAmId.JOINING.id, joiningHandler, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    }

    private def initIntroduceHandle(clientService: UcxWorkerService): Unit = {
        introduceHandler = new UcpAmRecvCallback {
            override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) = {
                val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
                val count = header.getInt

                val socketBuffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
                val members = (0 until count).map { _ => {
                    val socketAddress = Utils.newInetSocketAddress4(socketBuffer)
                    val name = Utils.newString(socketAddress)
                    UcxWorkerService.serverSocket.getOrElseUpdate(name, socketAddress)
                    name
                }}

                Log.debug(s"Member $listener receive introduce ${members}")

                if (bSequentialConnecting) {
                    clientService.executors.foreach { x => members.foreach(x.connecting(_)) }
                } else {
                    clientService.executors.foreach { x =>
                        x.submit(newTaskFor(new Runnable {
                            override def run = members.foreach(x.connecting(_))
                        }, Unit))
                    }
                }
                UcsConstants.STATUS.UCS_OK
            }
        }
        listener.worker.setAmRecvHandler(
            UcxAmId.INTRODUCE.id, introduceHandler, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    }

    def connectInSequential(isSeq: Boolean = true): Unit = bSequentialConnecting = isSeq

    def joiningCluster(): Unit = {
        UcxWorkerService.leaderSocket.keys.foreach(listener.following(_))
    }

    def run = {
        executors.foreach(_.start)
        Option(listener) match {
            case Some(t) => t.start
            case None => ()
        }
    }

    def close(timeout: Long = 1L, unit: TimeUnit = TimeUnit.MILLISECONDS) = {
        executors.foreach (x => {
            x.interrupt()
            x.worker.signal()
            x.join(unit.toMillis(timeout))
            x.close()
        })
        bShutDown = true
        bTermed = true
    }

    @inline
    def selectRR = {
        val i = (id.getAndIncrement % executors.size).abs
        executors(i)
    }

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

    override def execute(task: Runnable) = submit(task)

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
    val leaderSocket = new TrieMap[String, InetSocketAddress]
    val serverSocket = new TrieMap[String, InetSocketAddress]
    val clientWorker = new TrieMap[String, ByteBuffer]
}

object UcxAmId extends Enumeration { 
    val CONNECTION = Value
    val JOINING = Value
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