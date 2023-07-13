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

class UcxWorkerWrapper(val worker: UcpWorker, id: Long = 0, name: String = "") extends Thread {
    // private val hostName = InetAddress.getLocalHost.getHostName
    private val uniName = if (!name.isEmpty) name else s"${ManagementFactory.getRuntimeMXBean.getName}#$id"

    private var taskLimit: Semaphore = null
    private val taskQueue = new ConcurrentLinkedQueue[Runnable]()

    private val connections = new TrieMap[String, UcpEndpoint]
    private var connectionHandler: UcpListenerConnectionHandler = _

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
            val workerAddress = UcxWorkerService.clientWorker(host)

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
        connections.getOrElseUpdate(host, {
            val socketAddress = UcxWorkerService.serverSocket(host)
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
            ep
        })
    }

    def connecting(host: String) = {
        val header = ByteBuffer.allocateDirect(uniName.size)
        val workerAddress = worker.getAddress
        header.put(uniName.getBytes)
        header.rewind()

        getOrConnect(host).sendAmNonBlocking(UcxAmId.CONNECTION.id,
            UcxUtils.getAddress(header), header.limit(),
            UcxUtils.getAddress(workerAddress), workerAddress.limit(),
            UcpConstants.UCP_AM_SEND_FLAG_EAGER,
            new UcxCallback {
                override def onSuccess(request: UcpRequest): Unit = {
                    header.clear()
                    workerAddress.clear()
                }
            }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }

    def getOrFollow(host: String): UcpEndpoint = {
        connections.getOrElseUpdate(host, {
            val socketAddress = UcxWorkerService.leaderSocket(host)
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

            Log.debug(s"Member $this following to leader($host, $socketAddress)")

            worker.newEndpoint(endpointParams)
        })
    }

    def following(leader: String): Unit = {
        val socketAddress = UcxWorkerService.listenSocket(uniName)
        val (port, address) = Utils.newInetSocketTuple(socketAddress)

        val headerSize = uniName.size
        val bodySize = UnsafeUtils.INT_SIZE + address.size
        val buf = ByteBuffer.allocateDirect(headerSize + bodySize)
        val bufAddress = UcxUtils.getAddress(buf)

        buf.put(uniName.getBytes)
        buf.putInt(port)
        buf.put(address)
        buf.rewind()

        getOrFollow(leader).sendAmNonBlocking(UcxAmId.JOINING.id,
            bufAddress, headerSize, bufAddress + headerSize, bodySize,
            UcpConstants.UCP_AM_SEND_FLAG_EAGER,
            new UcxCallback {
                override def onSuccess(request: UcpRequest): Unit = {
                    buf.clear()
                }
            }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }

    def introduce(hostName: String, socketBuffer: ByteBuffer)= {
        // existing members connect to new member
        val headerSize = UnsafeUtils.INT_SIZE
        val bodySize = socketBuffer.limit
        val buf = ByteBuffer.allocateDirect(headerSize + bodySize)
        val bufAddress = UcxUtils.getAddress(buf)

        socketBuffer.rewind()
        buf.putInt(1)
        buf.put(socketBuffer)
        buf.rewind()

        Log.trace(s"Introduce $hostName to members")

        val eps = connections.filterKeys(_ != hostName)
        eps.foreach { case (_, ep) => {
                // existing members connect to new member
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

    def introduceAll(hostName: String): Unit = {
        val memberSockets = UcxWorkerService.serverSocket.filterKeys(_ != hostName)
        val members = memberSockets.values.map(Utils.newInetSocketTuple(_))

        if (members.isEmpty) {
            return
        }

        val headerSize = UnsafeUtils.INT_SIZE
        val bodySize = members.map {
            case (_, socketBytes) => UnsafeUtils.INT_SIZE + socketBytes.size }.sum
        val buf = ByteBuffer.allocateDirect(headerSize + bodySize)
        val bufAddress = UcxUtils.getAddress(buf)

        buf.putInt(members.size)
        members.foreach { case (port, socketAddress) => {
            buf.putInt(port)
            buf.put(socketAddress)
        }}

        Log.trace(s"Introduce members ${members} to new($hostName)")

        val ep = connections(hostName)
        ep.sendAmNonBlocking(UcxAmId.INTRODUCE.id,
            bufAddress, headerSize, bufAddress + headerSize, bodySize, 0,
            new UcxCallback {
                override def onSuccess(request: UcpRequest): Unit = {
                    buf.clear()
                }
            }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }

    def getConnectionHandle: UcpListenerConnectionHandler = {
        Option(connectionHandler) match {
            case Some(handle) => handle
            case None => {
                connectionHandler = new UcpListenerConnectionHandler {
                    override def onConnectionRequest(req: UcpConnectionRequest) = {
                        val name = Utils.newString(req.getClientAddress)

                        Log.debug(s"Listener $this receive connecting from ${name}")

                        val errHandle = new UcpEndpointErrorHandler {
                            override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
                                if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                                    Log.warn(s"Connection closed on ep: $ucpEndpoint")
                                } else {
                                    Log.error(s"Ep $ucpEndpoint got an error: $errorString")
                                }
                                connections.remove(name)
                                ucpEndpoint.close()
                            }
                        }
                        val ep = worker.newEndpoint(
                            new UcpEndpointParams()
                                .setConnectionRequest(req)
                                .setPeerErrorHandlingMode()
                                .setErrorHandler(errHandle)
                                .setName(s"Endpoint to ${req.getClientId}")
                        )
                        connections.put(name, ep)
                    }
                }
                connectionHandler
            }
        }
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
        UcxWorkerWrapper.set(this)
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
        UcxWorkerWrapper.set(null)
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
        Log.debug((s"Worker-$id closing"))
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

object UcxWorkerWrapper {
    private val localWorker = new ThreadLocal[UcxWorkerWrapper]
    private def set(worker: UcxWorkerWrapper) = {
        localWorker.set(worker)
    }
    def get: UcxWorkerWrapper = localWorker.get
}