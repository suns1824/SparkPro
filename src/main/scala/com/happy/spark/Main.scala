package com.happy.spark

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val count = 0
    //val sc = SparkSession.builder().master("local").appName("example").getOrCreate()
    val strategies = Seq("vmfa") ++ Seq(4, 5)
    val candidates = strategies.iterator.flatMap(_.hashCode().toString.toSeq)
    println(candidates.length)
    candidates.foreach(print(_))
  }

}
