package com.happy.spark.study.concurrent

import scala.collection._

object GuardedBlock extends App {
  val lock = new AnyRef
  var message: Option[String] = None
  val greeter = thread {
    lock.synchronized {
      while (message == None) lock.wait()
      log(message.get)
    }
  }
  lock.synchronized{
    message = Some("Hello!")
    lock.notify()
  }
  greeter.join()

  def thread(body: => Unit): Thread = {
    val t = new Thread {
      override def run() = body
    }
    t.start()
    t
  }

  def log(msg: String): Unit =
    println(s"${Thread.currentThread().getName}: $msg")
}

object SynchronizedPool extends App {
  private val tasks = mutable.Queue[() => Unit]()
  object Worker extends Thread {
    setDaemon(true)
    def poll() = tasks.synchronized {
      while (tasks.isEmpty) tasks.wait()
      tasks.dequeue()
    }

    override def run() = while (true) {
      val task = poll()
      task()
    }
  }
  Worker.start()
  def asynchronous(body: => Unit) = tasks.synchronized {
    tasks.enqueue(()=> body)
    tasks.notify()
  }
  asynchronous(println(s"${Thread.currentThread().getName}: Hello"))
  asynchronous(println(s"${Thread.currentThread().getName}: world"))
  Thread.sleep(500)
}



