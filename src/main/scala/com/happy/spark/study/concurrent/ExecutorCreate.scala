package com.happy.spark.study.concurrent

import scala.concurrent._
import java.util.concurrent.TimeUnit

object ExecutorCreate {
  val executor = new forkjoin.ForkJoinPool
  executor.execute(new Runnable {
    def run() = println("this task is run async")
  })
  //Thread.sleep(500)
  executor.shutdown()
  executor.awaitTermination(60, TimeUnit.SECONDS)
}

object Whatever extends App{
  def execute(body: => Unit) = ExecutionContext.global.execute(
    new Runnable {
      def run() = body
    }
  )

  lazy val obj = new AnyRef
  lazy val non = s"made by ${Thread.currentThread().getName}"
  execute{
    println(s"${Thread.currentThread().getName}:EC sees obj = $obj")
    println(s"${Thread.currentThread().getName}:EC sees non = $non")
  }
  println(s"${Thread.currentThread().getName}:Main sees obj = $obj")
  println(s"${Thread.currentThread().getName}:Main sees obj = $non")
}

