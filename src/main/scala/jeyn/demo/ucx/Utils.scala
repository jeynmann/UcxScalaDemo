package jeyn.demo.ucx

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.log4j.LogManager

import sun.nio.ch.DirectBuffer

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Locale
import java.math.{MathContext, RoundingMode}

import scala.util.control.NonFatal

trait Logging {
  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  @transient private var log_ : Logger = null
}

object FnUtils extends Logging {
  type Fn = () => Unit
  type Catcher = (Throwable) => Unit

  val catchToLogDebug = (e: Throwable) => logDebug("", e)
  val catchToLogError = (e: Throwable) => logError("", e)

  def makeRunnable(fn: Fn, err: Catcher = catchToLogDebug): Runnable = {
    new Runnable {
      override def run = try {
        fn()
      } catch {
        case e: Throwable => err(e)
      }
    }
  }
}

object BufferUtils {
  private val clazz = Class.forName("java.nio.DirectByteBuffer")
  private val constructor = clazz.getDeclaredConstructor(classOf[Long], classOf[Int])
  constructor.setAccessible(true)

  def makeByteBuffer(address: Long, length: Int): ByteBuffer with DirectBuffer = {
    constructor.newInstance(address.asInstanceOf[Object], length.asInstanceOf[Object])
      .asInstanceOf[ByteBuffer with DirectBuffer]
  }

  def address(buffer: ByteBuffer): Long = {
    buffer.asInstanceOf[sun.nio.ch.DirectBuffer].address
  }
}

object IOUtils extends Logging {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }
}

object StringUtils {
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    if (size >= BigInt(1L << 11) * EB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EB) {
          (BigDecimal(size) / EB, "EB")
        } else if (size >= 2 * PB) {
          (BigDecimal(size) / PB, "PB")
        } else if (size >= 2 * TB) {
          (BigDecimal(size) / TB, "TB")
        } else if (size >= 2 * GB) {
          (BigDecimal(size) / GB, "GB")
        } else if (size >= 2 * MB) {
          (BigDecimal(size) / MB, "MB")
        } else if (size >= 2 * KB) {
          (BigDecimal(size) / KB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }
}

object Utils {
  val INT_SIZE: Int = 4
  val LONG_SIZE: Int = 8
}
