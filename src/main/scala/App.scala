import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import training.Train
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
//import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.tree.GradientBoostedTrees
//import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import scala.util.{Success, Try}


object App {

  def main(args: Array[String]) :Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Spark starter project")
    sparkConf.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    /*println("Create RDD")
    val rdd = sc.parallelize(List(1,2,3,4,5,6))
    val list = rdd.collect()
    println(list.mkString(","))*/

    // Retrieve data:
    val spark = SparkSession
      .builder()
      .appName("Spark starter project")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()



    //val tweetData = sc.textFile("/Users/anand/Downloads/data.txt").toDF()

    val tweetDF = spark.read.text("/Users/anand/Downloads/data.txt")
    //tweetDF.withColumnRenamed("value", "msg")
    tweetDF.show()

    println("Total Messages:" +tweetDF.count())

    var happyMessages = tweetDF.filter(tweetDF("value").contains("happy"))
    val countHappy = happyMessages.count()
    println("Number of happy messages: " +  countHappy)

    var unhappyMessages = tweetDF.filter(tweetDF("value").contains(" sad"))
    val countUnhappy = unhappyMessages.count()
    println("Unhappy Messages: " + countUnhappy)

    //creating a dataset with equal number of good and bad tweets, for model to avoid biasing.
    val equalNoOfTweets = Math.min(countHappy, countUnhappy).toInt
    var tweets = happyMessages.limit(equalNoOfTweets).union(unhappyMessages.limit(equalNoOfTweets))

    val messageRDD = tweets.rdd
    val goodAndBadRecords = messageRDD.map(
      row =>{
        Try{

          val msgRow = row(0).toString.toLowerCase()
          var isHappy:Int = 0
          if (msgRow.contains("happy")){
            isHappy=1
          }
          else if(msgRow.contains(" sad")){
            isHappy=0
          }
          var msgFiltered = msgRow.replaceAll("happy", "")
          msgFiltered = msgFiltered.replaceAll(" sad", "")

          (isHappy, msgFiltered.split(" ").toSeq)

        }

      }


    )

    val exceptions = goodAndBadRecords.filter(_.isFailure)
    println("total records with exceptions: " + exceptions.count())
    exceptions.take(10).foreach(x => println(x.failed))

    var goodBadTweets = goodAndBadRecords.filter((_.isSuccess)).map(_.get)
    println("total records with successes: " + goodBadTweets.count())

    //goodBadTweets.take(5).foreach(x => println(x))

    // Launch training
    //Train.run()


    // Launch scoring
    // Score.run()
  }

}
