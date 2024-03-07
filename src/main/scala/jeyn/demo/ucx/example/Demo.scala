package jeyn.demo.ucx.example

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.openucx.jucx.NativeLibs
import jeyn.demo.ucx._
import java.util.Scanner
import java.util.concurrent.atomic.AtomicBoolean
import java.net.InetSocketAddress
import java.nio.ByteBuffer

object Demo extends Logging {
    val stopping = new AtomicBoolean
    val running = new AtomicBoolean

    def main(args:Array[String]) = {
        val parser = new GnuParser()
        val options = new Options()

        options.addOption("a", "address", true, "remote hosts. Format: host1:port1,host2:port2. Default: ")
        options.addOption("b", "bind", true, "listener address. Default: 0.0.0.0:3000")
        options.addOption("c", "num-clients", true, "Number of clients. Default: 16")
        options.addOption("d", "message-size", true, "size of message to transfer. Default: 4194304")
        options.addOption("f", "num-reqs-inflight", true, "number of requests in flight. Default: 16")
        options.addOption("h", "help", false, "display help message")
        options.addOption("n", "num-iterations", true, "number of iterations. Default: 99999999")
        options.addOption("q", "sequential-connect", true, "connects in sequential order. Default: 0")
        options.addOption("s", "num-servers", true, "Number of servers. Default: 16")
        options.addOption("x", "client-server", true, "launch both client and server. Default: 0")

        val cmd = parser.parse(options, args)
        if (cmd.hasOption("h")) {
            new HelpFormatter().printHelp("UcxScalaDemo", options)
            System.exit(0)
        }

        val host = cmd.getOptionValue("a","")
        val port = cmd.getOptionValue("b","0").toInt
        val msgSize = cmd.getOptionValue("d","4194304").toInt
        val iterations = cmd.getOptionValue("n","99999999").toInt
        val numFlights = cmd.getOptionValue("f","16").toInt
        val numClients = cmd.getOptionValue("c","16").toInt
        val numServers = cmd.getOptionValue("s","16").toInt
        val biDirection =cmd.getOptionValue("x","0").toInt

        println(s"host=${host}")
        println(s"port=${port}")
        println(s"iterations=${iterations}")
        println(s"msgSize=${msgSize}")
        println(s"numFlights=${numFlights}")
        println(s"numClients=${numClients}")
        println(s"numServers=${numServers}")

        NativeLibs.load()

        val transport = new UcxTransport()
        println(s"created transport.")
        if (!host.isEmpty()) {
            val namePort = host.split(":")
            val (name, port) = (namePort(0), namePort(1).toInt)
            transport.ucxEndpoint = transport.ucxWorker.newEndpoint()
            transport.ucxEndpoint.connect(new InetSocketAddress(name, port))
            println(s"created endpoint.")
        }
        if (port != 0) {
            transport.ucxListerner = transport.ucxWorker.newListener()
            transport.ucxListerner.bind(port)
            println(s"created listener.")
        }

        val msg = ByteBuffer.allocateDirect(msgSize)
        while (msg.remaining() > 0) {
            msg.putChar(('A' + (msg.remaining() & 15)).toChar)
        }

        val handle = new UcxHandler {
            override def onReceive(endpoint: UcxEndpoint, msg: ByteBuffer): Unit = {
                println(s"receive $msg from $endpoint")
            }
        }

        val task = new Thread {
            override def run = {
                while (!running.get()) { Thread.sleep(1000) }
                transport.ucxWorker.start()
                println(s"worker started.")

                if (transport.ucxEndpoint != null) {
                    transport.ucxEndpoint.setHandler(handle)
                }
                if (transport.ucxListerner != null) {
                    transport.ucxListerner.setHandler(handle)
                }

                while (!stopping.get()) {
                    if (transport.ucxListerner != null) {
                        println(s"Server running")
                    }
                    if (transport.ucxEndpoint != null) {
                        println(s"Client running")
                        transport.ucxEndpoint.send(msg)
                    }
                    Thread.sleep(1000)
                }

                transport.ucxWorker.close()
                println(s"worker closed.")
            }
        }
        task.start()

        val scanner = new Scanner(System.in)
        stopping.set(false)
        if (scanner.hasNextLine()) {
            val _ = scanner.nextLine()
        }
        running.set(true)
        if (scanner.hasNextLine()) {
            val _ = scanner.nextLine()
        }
        stopping.set(true)
    }
}