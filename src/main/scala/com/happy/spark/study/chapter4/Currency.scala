package com.happy.spark.study.chapter4

object Currency extends Enumeration {
  type Currency = Value
  val CNY, GBP, INR = Value
}
//Currency.values.foreach { currency => println(currency) }
