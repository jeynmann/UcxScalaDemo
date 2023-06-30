package org.openucx.ucx

import java.nio.ByteBuffer

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{RunnableFuture,FutureTask,Semaphore,TimeUnit,ConcurrentLinkedQueue,LinkedBlockingDeque}
import scala.collection.mutable.Map
import scala.collection.concurrent.TrieMap

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.openucx.jucx.NativeLibs

trait Monitor {
    def add(x: Int): Unit
    def aggregate(): Unit
}

class PpsMonitor(name: String, count: Int = 100) extends Monitor {
    private val aggrLocalRecord = new TrieMap[Long, Array[Int]]
    private val aggrLocalId = new TrieMap[Long, AtomicInteger]
    private val aggrOldId = new TrieMap[Long,Int]
    
    private val localRecord = new ThreadLocal[Array[Int]] {
        override def initialValue = {
            val record = new Array[Int](count)
            aggrLocalRecord.put(Thread.currentThread().getId, record)
            record
        }
    }

    private val localId = new ThreadLocal[AtomicInteger] {
        override def initialValue = {
            val id = new AtomicInteger()
            aggrOldId.put(Thread.currentThread().getId, 0)
            aggrLocalId.put(Thread.currentThread().getId, id)
            id
        }
    }

    override def add(x: Int) = {
        val id = localId.get.getAndIncrement
        localRecord.get((id % count).abs) = x
    }

    override def aggregate(): Unit = {
        aggrLocalId.foreach {
            case (t, atomicId) => {
                val id = atomicId.get
                if (aggrOldId(t) != id) {
                    aggrOldId.put(t, id)
                } else {
                    // aggrLocalRecord.remove(t)
                    // aggrLocalId.remove(t)
                    // aggrOldId.remove(t)
                }
            }
        }
        val records = {
            val tmp = new Array[Int](count * aggrLocalRecord.size)
            var i = 0
            aggrLocalRecord.foreach {
                case (_, record) => {
                    record.copyToArray(tmp, i)
                    i += count
                }
            }
            tmp.sorted
        }
        if (records.size == 0) {
            return 
        }
        val avg = records.sum / records.size
        val v50 = records((records.size - 1) * 50 / 100)
        val v80 = records((records.size - 1) * 80 / 100)
        val v99 = records((records.size - 1) * 99 / 100)
        Log.info(s"$name (average,50%,80%,99%)=($avg,$v50,$v80,$v99)")
    }
}

class SumMonitor(name: String) extends Monitor {
    private var timestamp: Long = System.currentTimeMillis
    private var sizestamp: Long = 0
    private val aggrLocalId = new TrieMap[Long, AtomicInteger]
    private val aggrOldId = new TrieMap[Long,Int]

    private val localId = new ThreadLocal[AtomicInteger] {
        override def initialValue = {
            val id = new AtomicInteger()
            aggrOldId.put(Thread.currentThread().getId, 0)
            aggrLocalId.put(Thread.currentThread().getId, id)
            id
        }
    }

    override def add(x: Int) = {
        val id = localId.get
        id.set(id.get + x)
    }

    override def aggregate() = {
        aggrLocalId.foreach {
            case (t, atomicId) => {
                val id = atomicId.get
                if (aggrOldId(t) != id) {
                    aggrOldId.put(t, id)
                } else {
                    // aggrLocalId.remove(t)
                    // aggrOldId.remove(t)
                }
            }
        }
        val sum = aggrOldId.values.sum
        val cur = System.currentTimeMillis
        val metric = (sum.toLong - sizestamp) * 1000 / (cur - timestamp)
        timestamp = cur
        sizestamp = sum
        Log.info(s"$name ${metric}")
    }
}

class MonitorThread(time: Int = 1000) extends Thread {
    private val monitors = new TrieMap[String, Monitor]
    private val sleeper = new Semaphore(0)

    setDaemon(true)
    setName(s"Monitor")

    def reg(name: String, monitor: Monitor) = {
        monitors.getOrElseUpdate(name, monitor)
    }

    def get(name: String) = {
        monitors(name)
    }

    def release = sleeper.release

    override def run() = {
        while(!isInterrupted) {
            sleeper.tryAcquire(time, TimeUnit.MILLISECONDS)
            val cur = System.currentTimeMillis
            monitors.values.foreach(_.aggregate)
        }
    }
}

object Global {
    val monitor = new MonitorThread()
    monitor.reg("ReadBW", new SumMonitor("Read (MB/s)"))
    monitor.reg("ReadLat", new PpsMonitor("Read (ms)", 100))
    monitor.start
}

class RecvMessage {
    var fid = 0
    var ucpAmData: UcpAmData = null

    def parse(headerAddress: Long, headerSize: Long, amData: UcpAmData) = {
        val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        fid = headerBuffer.getInt
        ucpAmData = amData
    }

    def process(callback: Runnable) = {
        if (ucpAmData.isDataValid) {
            callback.run
            ucpAmData.close
            UcsConstants.STATUS.UCS_OK
        } else {
            val amStartTime = System.nanoTime()
            val buff = ByteBuffer.allocateDirect(ucpAmData.getLength.toInt)
            val buffAddress = UcxUtils.getAddress(buff)
            UcxWorkerWrapper.get.worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, buffAddress, buff.limit,
                new UcxCallback() {
                    override def onSuccess(r: UcpRequest): Unit = {
                        Log.trace(s"AmHandleTime for flightId $fid is ${System.nanoTime() - amStartTime} ns")
                        callback.run
                        buff.clear
                    }
                }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            UcsConstants.STATUS.UCS_OK
        }
    }
}

class Client extends Thread {
    private val service = new UcxWorkerService()
    private val callbacks =  new TrieMap[Long, RunnableFuture[_]]

    val bwMonitor = Global.monitor.get("ReadBW")
    val latMonitor = Global.monitor.get("ReadLat")

    private val recvHandle = new UcpAmRecvCallback {
        override def onReceive(headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, ep: UcpEndpoint) = {
            val m = new RecvMessage()
            m.parse(headerAddress, headerSize, ucpAmData)
            val data = callbacks.remove(m.fid)
            if (data.isEmpty) {
                throw new UcxException(s"No data for flightId ${m.fid}.")
            }
            val callback = data.get
            m.process(callback)
            UcsConstants.STATUS.UCS_OK
        }
    }
    private val handles = Array[(Int, UcpAmRecvCallback, Long)](
        (UcxAmId.FETCH_REPLY.id, recvHandle, UcpConstants.UCP_AM_FLAG_WHOLE_MSG))

    private var flightLimit: Semaphore = null
    private var iterations = 0
    private var mesgSize = 0

    def init(n: Int, hostPort: String, numFlights:Int, iter: Int, size: Int) = {
        if (numFlights > 0) {
            flightLimit = new Semaphore(numFlights)
        }
        service.initClient(n, hostPort, handles)
        iterations = iter
        mesgSize = size
    }

    def fetch(fid: Int, host: String, expect: Int) = {
        val startTime = System.nanoTime()
        val worker = UcxWorkerWrapper.get
        val ep = worker.getOrConnect(host)

        val headerSize = UnsafeUtils.INT_SIZE + worker.workerName.size
        val bodySize = UnsafeUtils.INT_SIZE

        val buffer = ByteBuffer.allocateDirect(headerSize + bodySize)

        buffer.putInt(fid)
        buffer.put(worker.workerName.getBytes)
        buffer.putInt(expect)
        buffer.rewind()
        // callback if fetched
        callbacks.put(fid, new FutureTask(
            new Runnable {
                override def run = {
                    val timecost = System.nanoTime() - startTime
                    bwMonitor.add(expect >> 20)
                    latMonitor.add((timecost / 1000000).toInt)
                    Log.trace(s"Total time for flightId $fid size $expect is " + 
                        s"${timecost} ns")
                    Option(flightLimit) match {
                        case Some(sem) => sem.release
                        case None => ()
                    }
                }
            }, Unit))
        // send to fetch
        val haddress = UnsafeUtils.getAdress(buffer)
        ep.sendAmNonBlocking(UcxAmId.FETCH.id,
            haddress, headerSize, haddress + headerSize, bodySize,
            UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
            override def onSuccess(request: UcpRequest): Unit = {
                buffer.clear()
                Log.trace(s"Sent message $fid to $host on $ep to fetch size $expect " +
                    s"in ${System.nanoTime() - startTime} ns")
            }
        }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }

    override def run() = {
        service.run
        val flight = new AtomicInteger
        for (i <- 0 until iterations) {
            while (!isInterrupted) {
                UcxWorkerService.serverSocket.keys.flatMap { x => {
                    Option(flightLimit) match {
                        case Some(sem) => sem.acquire
                        case None => ()
                    }
                    Seq(service.submit(new Runnable {
                        override def run = fetch(flight.getAndIncrement, x, mesgSize)
                    }))
                }}
            }
        }
        service.shutdown
    }

    def close() = {
        service.close()
    }
}

class FetchMessage {
    private val random = new Random

    var fid = 0
    var workerName = ""
    var body = 0
    var ucpAmData: UcpAmData = null

    def parse(headerAddress: Long, headerSize: Long, amData: UcpAmData) = {     // parse data
        val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val message = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)

        fid = header.getInt
        workerName = java.nio.charset.StandardCharsets.UTF_8.decode(header).toString
        body = message.getInt
        ucpAmData = amData
    }

    def process = {
        try {
            val startTime = System.nanoTime()
            // println("before")
            // ucpAmData.close
            // println("after")
            // allocate data
            val headerSize = UnsafeUtils.INT_SIZE
            val bodySize = body
            val buffer = ByteBuffer.allocateDirect(headerSize + bodySize)
            // prepare data
            buffer.putInt(fid)

            val message = new Array[Byte](body)
            random.nextBytes(message)
            buffer.put(message)
            buffer.rewind
            //  send fetch reply
            val worker = UcxWorkerWrapper.get
            val ep = worker.getOrConnectBack(workerName)
            val address = UnsafeUtils.getAdress(buffer)
            ep.sendAmNonBlocking(UcxAmId.FETCH_REPLY.id,
                address, headerSize, address + headerSize, bodySize, 0,
                new UcxCallback {
                    override def onSuccess(request: UcpRequest): Unit = {
                        buffer.clear()
                        Log.trace(s"Sent message $fid to $workerName on $ep with size $bodySize " +
                            s" in ${System.nanoTime() - startTime} ns.")
                    }

                    override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                        Log.warn(s"Failed to fetch $errorMsg")
                    }
                }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        } catch {
            case ex: Throwable => Log.error(s"Failed to read and fetch data: $ex")
        }
    }
}

class Server extends Thread {
    private val outStandings =  new LinkedBlockingDeque[FetchMessage]
    private val service = new UcxWorkerService()

    val fetchHandle = new UcpAmRecvCallback {
        override def onReceive(headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) = {
            val m = new FetchMessage()
            m.parse(headerAddress, headerSize, amData)
            outStandings.offer(m)
            // submit reply task
            UcsConstants.STATUS.UCS_OK
        }
    }
    private val handles = Array[(Int, UcpAmRecvCallback, Long)](
        (UcxAmId.FETCH.id, fetchHandle, UcpConstants.UCP_AM_FLAG_WHOLE_MSG))
    // UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | 

    def init(n: Int, hostPort: String) = {
        service.initServer(n, hostPort, handles)
    }

    override def run() = {
        service.run
        while (!isInterrupted) {
            Option(outStandings.poll) match {
                case Some(msg) => service.submit(new Runnable {
                    override def run = msg.process
                })
                case None => {}
            }
        }
        service.shutdown
    }

    def close() = {
        service.close()
    }
}

object Demo {
    def main(args:Array[String]) = {
        val argsMap = parseArgs(args)
        val listen = argsMap.getOrElseUpdate("p", "3000");
        val hosts = argsMap.getOrElseUpdate("s","");
        val iterations = argsMap.getOrElseUpdate("n","99999999").toInt;
        val msgSize = argsMap.getOrElseUpdate("d", "4194304").toInt;
        val numFlights = argsMap.getOrElseUpdate("f", "16").toInt;
        val numClients = argsMap.getOrElseUpdate("cli", "16").toInt;
        val numServers = argsMap.getOrElseUpdate("srv", "16").toInt;

        println(s"listen=${listen}")
        println(s"hosts=${hosts}")
        println(s"iterations=${iterations}")
        println(s"msgSize=${msgSize}")
        println(s"numClients=${numClients}")
        println(s"numServers=${numServers}")
        println(s"numFlights=${numFlights}")

        NativeLibs.load()

        val server = if (!listen.isEmpty) {
            val srv = new Server
            srv.init(numServers, listen)
            if (!hosts.isEmpty)
                srv.start
            else
                srv.run
            srv
        } else {
            null
        }

        val client = if (!hosts.isEmpty) {
            val cli = new Client
            cli.init(numClients, hosts, numFlights, iterations, msgSize)
            cli.run
            cli
        } else {
            null
        }

        Option(client) match {
            case Some(cli) => cli.close
            case None => {}
        }

        Option(server) match {
            case Some(srv) => srv.close
            case None => {}
        }

        Global.monitor.interrupt
        Global.monitor.release
        Global.monitor.join(10)
    }

    def parseArgs(args: Array[String]): Map[String,String] = {
        val argsMap = Map.empty[String,String]
        for (arg <- args) {
            if (arg.contains("h")) {
                println(DESCRIPTION)
                return argsMap
            }
        }
        for (arg <- args) {
            val kv = arg.split("=")
            if (kv.size == 2) {
                argsMap.put(kv(0), kv(1))
            } else if (kv.size == 1) {
                argsMap.put(kv(0), "")
            }
        }
        argsMap
    }

    val DESCRIPTION = "JUCX benchmark.\n" +
        "Run: \n" +
        "scala -jar target/ucx-demo-0.1-for-default-jar-with-dependencies.jar" +
        "[p=port] [s=host1:port1,host2:port2] [n=number of iterations] [d=size to transfer] \n\n" +
        "Parameters:\n" +
        "h - print help\n" +
        "s - IP address to bind fetcher listener (default: 0.0.0.0)\n" +
        "p - port to bind fetcher listener (default: 54321)\n" +
        "d - total size in bytes to transfer from fetcher to receiver (default 10000)\n" +
        "n - number of iterations (default 5)\n";
}