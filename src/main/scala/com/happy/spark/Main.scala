package com.happy.spark

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().master("local").appName("example").getOrCreate()
  }

}
