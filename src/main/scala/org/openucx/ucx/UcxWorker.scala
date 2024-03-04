package org.openucx.ucx

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, ConcurrentLinkedQueue, Semaphore}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Seq

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}

class UcxWorker(val worker: UcpWorker, id: Long = 0) extends Thread {
    // private val hostName = InetAddress.getLocalHost.getHostName
    private val uniName = s"${ManagementFactory.getRuntimeMXBean.getName}#$id"

    private var taskLimit: Semaphore = null
    private val taskQueue = new ConcurrentLinkedQueue[Runnable]()

    private val connections = new TrieMap[String, UcpEndpoint]

    setDaemon(true)
    setName(s"Worker-$id")

    def workerName = uniName

    def initService(handles: Array[(Int, UcpAmRecvCallback, Long)]) = {
        Option(handles) match {
            case Some(h) => h.foreach {
                case (id, callback, flags) => worker.setAmRecvHandler(id, callback, flags)
            }
            case None => {}
        }
    }

    def isTaskFull() = {
        Option(taskLimit) match {
            case Some(sem) => sem.availablePermits == 0
            case None => false
        }
    }

    def initTaskLimit(numTasks: Int) = {
        Option(taskLimit) match {
            case Some(limit) => {}
            case None => { taskLimit = new Semaphore(numTasks) }
        }
    }

    def getOrConnectBack(host: String): UcpEndpoint = {
        connections.getOrElseUpdate(host,  {
            val workerAddress = UcxService.clientWorker(host)

            Log.debug(s"Server $this connecting to client($host, $workerAddress)")

            worker.newEndpoint(new UcpEndpointParams()
                .setName(s"Server to $host")
                .setUcpAddress(workerAddress)
                .setErrorHandler(
                new UcpEndpointErrorHandler() {
                    override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
                        Log.warn(s"Server to $host got an error: $errorMsg")
                        connections.remove(host)
                    }
                }))
        })
        
    }

    def getOrConnect(host: String): UcpEndpoint = {
        connections.getOrElseUpdate(host,  {
            val socketAddress = UcxService.serverSocket(host)
            val endpointParams = new UcpEndpointParams()
                .setPeerErrorHandlingMode()
                .setSocketAddress(socketAddress)
                .sendClientId()
                .setErrorHandler(
                new UcpEndpointErrorHandler() {
                    override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
                        Log.warn(s"Client to $host got an error: $errorMsg")
                        connections.remove(host)
                    }
                }).setName(s"Client to $host")

            Log.debug(s"Client $this connecting to server($host, $socketAddress)")

            val ep = worker.newEndpoint(endpointParams)
            val header = ByteBuffer.allocateDirect(uniName.size)
            val workerAddress = worker.getAddress
            header.put(uniName.getBytes)
            header.rewind()

            ep.sendAmNonBlocking(UcxAmId.CONNECTION.id,
                UcxUtils.getAddress(header), header.limit(),
                UcxUtils.getAddress(workerAddress), workerAddress.limit(),
                UcpConstants.UCP_AM_SEND_FLAG_EAGER,
                new UcxCallback {
                    override def onSuccess(request: UcpRequest): Unit = {
                        header.clear()
                        workerAddress.clear()
                    }
                }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            ep
        })
    }

    def introduceListener(host: String): Unit = {
        connections.foreach { case (name, ep) => {
            val socketAddress = UcxService.listenSocket(host)
            val hostAddress = socketAddress.getAddress.getHostAddress
            val hostPort = socketAddress.getPort
            
            Log.debug(s"Client $this introduce ($host) to server ($name)")
            
            val headerSize = host.size
            val bodySize = UnsafeUtils.INT_SIZE + hostAddress.size
            val buf = ByteBuffer.allocateDirect(headerSize + bodySize)
            val bufAddress = UcxUtils.getAddress(buf)
            buf.put(host.getBytes)
            buf.putInt(hostPort)
            buf.put(hostAddress.getBytes)
            buf.rewind()
            
            ep.sendAmNonBlocking(UcxAmId.INTRODUCE.id,
                bufAddress, headerSize, bufAddress + headerSize, bodySize,
                UcpConstants.UCP_AM_SEND_FLAG_EAGER,
                new UcxCallback {
                    override def onSuccess(request: UcpRequest): Unit = {
                        buf.clear()
                    }
                }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        }}
    }

    // def send(host: String, header: ByteBuffer, body: ByteBuffer, callback: UcxCallback,
    //     flags:Long = 0, memoryType:Int = MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) = {
    //     val hAddress = UnsafeUtils.getAdress(header)
    //     val bAddress = UnsafeUtils.getAdress(body)
    //     getOrConnect(host).sendAmNonBlocking(UcxAmId.FETCH.id, hAddress, header.limit, bAddress, body.limit,
    //         flags, callback, memoryType)
    // }

    // def send(ep: UcpEndpoint, header: ByteBuffer, body: ByteBuffer, callback: UcxCallback,
    //     flags:Long = 0, memoryType:Int = MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) = {
    //     val hAddress = UnsafeUtils.getAdress(header)
    //     val bAddress = UnsafeUtils.getAdress(body)
    //     ep.sendAmNonBlocking(UcxAmId.FETCH.id, hAddress, header.limit, bAddress, body.limit,
    //         flags, callback, memoryType)
    // }

    override def run(): Unit = {
        Log.debug((s"Worker-$id start"))
        UcxWorker.set(this)
        while (!isInterrupted) {
            Option(taskQueue.poll()) match {
                case Some(task) => {
                    task.run
                    release
                }
                case None => {}
            }
            while (worker.progress() != 0) {}
            if (taskQueue.isEmpty) {
                worker.waitForEvents()
            }
        }
        UcxWorker.set(null)
        Log.debug((s"Worker-$id stop"))
    }

    @inline
    def acquire(): Unit = Option(taskLimit) match {
        case Some(sem) => sem.acquire
        case None => ()
    }

    @inline
    def release(): Unit = Option(taskLimit) match {
        case Some(sem) => sem.release
        case None => ()
    }

    @inline
    def submit(task: Runnable) = {
        acquire
        taskQueue.offer(task)
        worker.signal()
    }

    @inline
    def close(): Unit = {
        Log.debug((s"Worker-$id close"))
        interrupt
        join(10)
        val reqs = connections.map {
            case (_, endpoint) => endpoint.closeNonBlockingForce()
        }
        while (!reqs.forall(_.isCompleted)) {
            worker.progress
        }
        connections.clear()
        worker.close()
        Log.debug((s"Worker-$id closed"))
    }
}

object UcxWorker {
    private val localWorker = new ThreadLocal[UcxWorker]
    private def set(worker: UcxWorker) = {
        localWorker.set(worker)
    }
    def get: UcxWorker = localWorker.get
}