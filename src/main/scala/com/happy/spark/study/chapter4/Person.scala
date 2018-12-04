package com.happy.spark.study.chapter4

class Person(val firstName: String, val lastName: String) {
  var position: String = _
  println(s"Creating $toString")

  def this(firstName: String, lastName: String, postionHeld: String) {
    this(firstName, lastName)
    position = postionHeld
  }
  override def toString: String = {
    s"$firstName $lastName holds $position position"
  }
}

