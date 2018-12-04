package com.happy.spark.sql
import java.util.logging.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import spire.implicits

/*
hdfs dfs -mkdir /user/root/examples
hdfs dfs -put xxx/people.json  /user/root/examples
 */

object ReadPeople {
  def main(args: Array[String]): Unit = {
    /*
   {"name:""Andy", "job number": "002", "age":10, "gender": "male", "depId": 1, "salary": 15000}
    */
    Logger.getLogger("org.apache.spark").setLevel(Level.WARNING)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARNING)
    val spark = SparkSession.builder.appName("ReadPeople").master("yarn-client").getOrCreate()
    val sqlContext = spark.sqlContext
    //1
    val df = sqlContext.read.json("examples/people.json")
    df.show()
    df.select("name")
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    //2
    val people = sqlContext.read.json("hdfs:/user/root/examples/people.json")
    val dept = sqlContext.read.load("hdfs:/user/root/examples/department.json")
    people.show()
    print(people.toJSON.collect())
    //people.take(3)
    //people.columns
    people.where("gender='male'").count()
    people.filter("age>28").show()
  }
}
