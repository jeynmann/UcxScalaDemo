package jeyn.demo.ucx

import org.openucx.jucx.ucp.UcpWorker

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue

class NamedThread(name: String, daemon: Boolean, r: Runnable = null) extends Thread(r) {       
  setName(s"${name}-${super.getName}")
  setDaemon(daemon)
}

class NamedFactory extends ThreadFactory {
  private var daemon: Boolean = true
  private var prefix: String = "UCX"

  def setDaemon(isDaemon: Boolean): this.type = {
    daemon = isDaemon
    this
  }

  def setPrefix(name: String): this.type = {
    prefix = name
    this
  }

  def newThread(r: Runnable): Thread = {
    new NamedThread("UCX", true, r)
  }
}

class ProgressThread(worker: UcpWorker, useWakeup: Boolean,
                     name: String = "UCX-progress")
extends NamedThread(name, true) {
  protected val running = new AtomicBoolean(true)

  def close(cleanTask: Runnable): Unit = {
    if (running.compareAndSet(true, false)) {
      worker.signal()
    }
    cleanTask.run()
    worker.close()
  }

  override def run(): Unit = {
    val doAwait = if (useWakeup) worker.waitForEvents _ else () => {}
    while (running.get()) {
      while (worker.progress != 0) {}
      doAwait()
    }
  }
}

class WorkerThread(worker: UcpWorker, useWakeup: Boolean,
                   name: String = "UCX-wroker")
extends ProgressThread(worker, useWakeup, name) {
  protected val taskQueue = new ConcurrentLinkedQueue[Runnable]

  @`inline`
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    worker.signal()
  }

  @`inline`
  def await() = {
    if (taskQueue.isEmpty) {
      worker.waitForEvents()
    }
  }

  override def run(): Unit = {
    val doAwait = if (useWakeup) await _ else () => {}
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (worker.progress != 0) {}
      doAwait()
    }
  }
}