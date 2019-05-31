import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

/**
  * Created by qpangzi on 2018/10/27.
  */
object Movierec {

    case class Movie(movieId: Int, title: String)
    case class User(userId: Int, gender: String, age: Int, occupation: Int, zipCode: String)

    def parseMovieData(data: String): Movie = {
      val dataField = data.split(",")
        //先不断言，这里需要做数据清洗或者数据统一格式化
      //assert(dataField.size == 3)
      Movie(dataField(0).toInt, dataField(1))
    }

    def parseUserData(data: String): User = {
      val dataField = data.split(",")
      //assert(dataField.size == 5)
      User(dataField(0).toInt, dataField(1).toString, dataField(2).toInt, dataField(3).toInt, dataField(4).toString)
    }

    def parseRatingData(data: String): Rating = {
      val dataField = data.split(",")
      Rating(dataField(0).toInt, dataField(1).toInt, dataField(2).toDouble)
    }

    def main(args: Array[String]){

      val spark = SparkSession.builder.master("yarn-client").appName("MovieRec").getOrCreate()
      import spark.implicits._

      var moviesData = spark.read.textFile("File:///usr/bigdata/data/ml-1w/movies.csv").map(parseMovieData _).cache()

      //var usersData = spark.read.textFile("File:///usr/bigdata/data/ml-1w/users.csv").map(parseUserData _).cache()
      var ratingsData = spark.read.textFile("File:///usr/bigdata/data/ml-1w/ratings.csv").map(parseRatingData _).cache()

      val moviesDF = moviesData.toDF()
      //val usersDF = usersData.toDF()
      val ratingsDF = ratingsData.toDF()

      val tempPartitions = ratingsData.randomSplit(Array(0.7, 0.3), 1024L)
      val trainingSetOfRatingsData = tempPartitions(0).cache().rdd
      val testSetOfRatingData = tempPartitions(1).cache().rdd

      // training model
      val recomModel = new ALS().setRank(20).setIterations(10).run(trainingSetOfRatingsData)
      //取最高的十位打印出来
      val recomResult = recomModel.recommendProducts(10, 10)
      println(s"Recommend Movie to User ID 10")
      println(recomResult.mkString("\n"))

      spark.stop()
    }

}
