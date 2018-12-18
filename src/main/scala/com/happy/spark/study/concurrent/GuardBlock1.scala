package com.happy.spark.study.concurrent

import scala.collection._

object GuardBlock1 extends App{

  private val tasks = mutable.Queue[() => Unit] ()

  // worker线程会处在忙等待状态
  def asynchronous(body: => Unit) = tasks.synchronized {
    tasks.enqueue(() => body)
  }
  asynchronous(GuardedBlock.log("hello"))
  asynchronous(GuardedBlock.log("world"))

  Thread.sleep(5000)

  val worker = new Thread {
    def poll(): Option[()=> Unit] = tasks.synchronized {
      if (tasks.nonEmpty) Some(tasks.dequeue()) else None
    }

    override def run(): Unit = while (true) poll() match {
      case Some(task) => task()
      case None =>
    }
  }
  worker.setName("Worker")
  worker.setDaemon(true)
  worker.start()

}
