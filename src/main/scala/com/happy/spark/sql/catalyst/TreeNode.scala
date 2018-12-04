package com.happy.spark.sql.catalyst

class TreeNode {
  // catalyst node location  locate sql
  case class Origin(line: Option[Int] = None, startPosition: Option[Int] = None)
  object CurrentOrigin {
    private val value = new ThreadLocal[Origin]() {
      override def initialValue(): Origin = new Origin()
    }
    def get: Origin = value.get()
    def set(o: Origin): Unit = value.set(o)
    def reset(): Unit = value.set(Origin())
    def setPosition(line: Int, start: Int): Unit = {
      value.set(value.get.copy(line = Some(line), startPosition = Some(start)))
    }
    def withOrigin[A](o: Origin)(f: => A): A = {
      set(o)
      val ret = try f finally { reset() }
      reset()
      ret
    }
  }

}
