package org.openucx.ucx

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

object Utils {
    @inline
    def newInetSocketAddress(hostPort: String) = {
        if (hostPort.contains(":")) {
            val host = hostPort.split(":")
            new InetSocketAddress(InetAddress.getByName(host(0)), host(1).toInt)
        } else {
            new InetSocketAddress(InetAddress.getByName("0"), hostPort.toInt)
        }
    }

    @inline
    def newInetSocketTuple(socket: InetSocketAddress) = {
        socket.getPort->socket.getAddress.getAddress
    }

    @inline
    def newInetSocketBuffer(socket: InetSocketAddress) = {
        val (port, address) = Utils.newInetSocketTuple(socket)
        val buffer = ByteBuffer.allocateDirect(UnsafeUtils.INT_SIZE + address.size)
        buffer.putInt(port)
        buffer.put(address)
        buffer
    }

    @inline
    def newInetSocketAddress4(buffer: ByteBuffer) = {
        val port = buffer.getInt
        val address = new Array[Byte](4)
        buffer.get(address)
        new InetSocketAddress(InetAddress.getByAddress(address), port)
    }

    @inline
    def newInetSocketAddress6(buffer: ByteBuffer) = {
        val port = buffer.getInt
        val address = new Array[Byte](16)
        buffer.get(address)
        new InetSocketAddress(InetAddress.getByAddress(address), port)
    }

    @inline
    def newString(socket: InetSocketAddress) = {
        s"${socket.getAddress.getHostAddress}:${socket.getPort}"
    }

    @inline
    def newString(buffer: ByteBuffer) = {
        StandardCharsets.UTF_8.decode(buffer).toString
    }

    @inline
    def newBytes(buffer: ByteBuffer) = {
        StandardCharsets.UTF_8.decode(buffer)
    }

    @inline
    def newBuffer(buffer: ByteBuffer) = {
        val buf = ByteBuffer.allocateDirect(buffer.remaining())
        buf.put(buffer)
        buf
    }
}
