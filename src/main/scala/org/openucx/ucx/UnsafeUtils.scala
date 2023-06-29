package org.openucx.ucx

import java.lang.reflect.InvocationTargetException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import sun.nio.ch.{DirectBuffer, FileChannelImpl}

import org.openucx.jucx.UcxException

object UnsafeUtils {
    val INT_SIZE: Int = 4
    val LONG_SIZE: Int = 8

    private val mmap = classOf[FileChannelImpl].getDeclaredMethod("map0", classOf[Int], classOf[Long], classOf[Long])
    mmap.setAccessible(true)

    private val unmmap = classOf[FileChannelImpl].getDeclaredMethod("unmap0", classOf[Long], classOf[Long])
    unmmap.setAccessible(true)

    private val classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer")
    private val directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(classOf[Long], classOf[Int])
    directBufferConstructor.setAccessible(true)

    def getByteBufferView(address: Long, length: Int): ByteBuffer with DirectBuffer = {
        directBufferConstructor.newInstance(address.asInstanceOf[Object], length.asInstanceOf[Object])
            .asInstanceOf[ByteBuffer with DirectBuffer]
    }

    def getAdress(buffer: ByteBuffer): Long = {
        buffer.asInstanceOf[sun.nio.ch.DirectBuffer].address
    }

    def mmap(fileChannel: FileChannel, offset: Long, length: Long): Long = {
        try {
            mmap.invoke(fileChannel, 1.asInstanceOf[Object], offset.asInstanceOf[Object], length.asInstanceOf[Object])
            .asInstanceOf[Long]
        } catch {
            case e: Exception =>
            Log.error(s"Failed to mmap (${fileChannel.size()} $offset $length): $e")
            throw new UcxException(e.getMessage)
        }
    }

    def munmap(address: Long, length: Long): Unit = {
        try {
            unmmap.invoke(null, address.asInstanceOf[Object], length.asInstanceOf[Object])
        } catch {
            case e@(_: IllegalAccessException | _: InvocationTargetException) =>
            Log.error(e.getMessage)
        }
    }
}
