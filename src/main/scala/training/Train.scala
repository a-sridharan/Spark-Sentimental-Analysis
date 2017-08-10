package training


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Train {

  def run(sparkSession: SparkSession, trainingData: RDD[LabeledPoint], validationData: RDD[LabeledPoint]):
  Tuple2[RDD[Tuple2[Double, Double]], RDD[Tuple2[Double, Double]]] = {

    // Training code goes here:
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(20) //number of passes over training data
    boostingStrategy.treeStrategy.setNumClasses(2) //two output classes: happy and sad
    boostingStrategy.treeStrategy.setMaxDepth(5) //depth of each tree

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    var predTrain = trainingData.map { point =>
      val prediction = model.predict(point.features)
      Tuple2(point.label, prediction)
    }

    var predValidation =
      validationData.map { point =>
      val prediction = model.predict(point.features)
      Tuple2(point.label, prediction)

    }

    (predTrain, predValidation)

}

}
