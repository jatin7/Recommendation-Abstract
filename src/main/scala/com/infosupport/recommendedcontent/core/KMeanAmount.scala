package com.infosupport.recommendedcontent.core

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._


/**
  * Created by giangtrinh on 8/8/17.
  */
object KMeanAmount {
  case class Train(cluster: Int)
  case class UserCluster(cluster:Int, userId:String, amount: Double, transformed_amount: Double)
  def props(sc: SparkContext) = Props(new KMeanAmount(sc))
}

class KMeanAmount(sc: SparkContext) extends Actor with ActorLogging {

  import KMeanAmount._

  def receive = {
    case Train(cluster: Int) => trainModel(cluster)
  }

  def splitAmountRange = udf(
    (d: Double) => {
      if(d <= 100000){
        1.0
      } else if (d > 100000 && d <= 200000){
        2.0
      } else if (d > 200000 && d <= 500000){
        3.0
      } else {
        4.0
      }
    }
  )

  def convertUserId = udf(
    (i: Int) => {
      s"0${i.toString}"
    }
  )

  /**
    * Trains the new recommender system model
    */
  private def trainModel(cluster: Int) = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._
    val transactionDataDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "transaction_data", "keyspace" -> "events"))
      .load()//.cache()

    val userIdDF = transactionDataDF.select("user_id", "original_amount").where("original_amount IS NOT NULL").withColumn("user_id", $"user_id".cast(IntegerType))
                      .withColumn("original_amount_transformed", splitAmountRange($"original_amount"))

    val assembler = new VectorAssembler()
      .setInputCols(Array("user_id", "original_amount_transformed"))
      .setOutputCol("features")

    val training = assembler.transform(userIdDF)

   val kmeans = new KMeans().setK(cluster).setMaxIter(50).setSeed(1L)
    val model = kmeans.fit(training)
    //val model = KMeans.train(rddVector, 4 ,10)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)


    val transformed = model.transform(training)
    transformed
      .select("user_id", "original_amount" , "prediction")
      .show(false)

    val selectedTranformed = transformed.select("user_id", "original_amount" , "original_amount_transformed", "prediction").withColumn("user_id", convertUserId($"user_id"))
    selectedTranformed.show(false)

    val rowRDD = selectedTranformed.map(p => UserCluster(p.getAs("prediction"), p.getAs("user_id"), p.getAs("original_amount"), p.getAs("original_amount_transformed"))).rdd

    rowRDD.saveToCassandra("events", "userid_by_amount_cluster")
    println(s"Within Set Sum of Squared Errors = $WSSSE")
  }
}
