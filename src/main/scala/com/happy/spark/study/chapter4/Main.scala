package com.happy.spark.study.chapter4

object Main {
  def main(args: Array[String]): Unit = {
    val arr = Array(1,2,3,4)
    val total = arr.map(i => i+1)
    var summer = 0
    for(i <- total)
      print(i)
    for(i <-0 to (arr.length-1))
      summer += arr(i)
    print(summer)
 //   (1 to 9).map("^" * _).foreach(i => println( i))
//    val john = new Person("John", "Smith", "Analyst")
//    println(john)
//    val bill = new Person("Bill", "Walker")
//    println(bill)

//    println(MarkerFactory.getMarker("blue"))
//    println(MarkerFactory getMarker "blue")
  }
}
