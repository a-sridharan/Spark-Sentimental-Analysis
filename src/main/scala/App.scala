import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import training.Train
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

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

    //transform code

    val hashingTF = new HashingTF(2000)
    val input_labeled= goodBadTweets.map(
      t => (t._1, hashingTF.transform(t._2)))
        .map(x => new LabeledPoint((x._1).toDouble, x._2))

    //input_labeled.take(5).foreach(println)
    var sample = goodBadTweets.take(1000).map(
      t => (t._1, hashingTF.transform(t._2), t._2))
      .map(x => (new LabeledPoint((x._1).toDouble, x._2), x._3))


    //fixing overfitting
    //splitting into training and validation sets

    val split = input_labeled.randomSplit(Array(0.7, 0.3))
    val (trainingData, validationData) = (split(0), split(1))
    // Launch training

    val (trainedPredictionRDD, validatedPredictionRDD) = Train.run(spark, trainingData, validationData)
    val result = trainedPredictionRDD.collect()

    var trainingHappyTotal = 0
    var trainingUnhappyTotal = 0
    var trainingHappyCorrect = 0
    var trainingUnhappyCorrect = 0
    result.foreach(
      r => {
        if(r._1 == 1){
          trainingHappyTotal += 1
        }
        else if (r._1 == 0){
          trainingUnhappyTotal += 1
        }
        if (r._1 == 1 && r._2 ==1) {
          trainingHappyCorrect += 1
        } else if (r._1 == 0 && r._2 == 0) {
          trainingUnhappyCorrect += 1
        }
      }

    )

    println("unhappy messages in Training Set: " + trainingUnhappyTotal + " happy messages: " + trainingHappyTotal)
    println("happy % correct: " + trainingHappyCorrect.toDouble/trainingHappyTotal)
    println("unhappy % correct: " + trainingUnhappyCorrect.toDouble/trainingUnhappyTotal)

    val trainingTestErr = trainedPredictionRDD.filter(r => r._1 != r._2).count.toDouble / trainingData.count()
    println("Test Error Training Set: " + trainingTestErr)

    val results = validatedPredictionRDD.collect()

    var validationHappyTotal = 0
    var validationUnhappyTotal = 0
    var validationHappyCorrect = 0
    var validationUnhappyCorrect = 0
    results.foreach(
      r => {
        if (r._1 == 1) {
          validationHappyTotal += 1
        } else if (r._1 == 0) {
          validationUnhappyTotal += 1
        }
        if (r._1 == 1 && r._2 ==1) {
          validationHappyCorrect += 1
        } else if (r._1 == 0 && r._2 == 0) {
          validationUnhappyCorrect += 1
        }
      }
    )
    println("unhappy messages in Validation Set: " + validationUnhappyTotal + " happy messages: " + validationHappyTotal)
    println("happy % correct: " + validationHappyCorrect.toDouble/validationHappyTotal)
    println("unhappy % correct: " + validationUnhappyCorrect.toDouble/validationUnhappyTotal)

    val validationTestErr = validatedPredictionRDD.filter(r => r._1 != r._2).count.toDouble / validationData.count()
    println("Test Error Validation Set: " + validationTestErr)

  }

}
