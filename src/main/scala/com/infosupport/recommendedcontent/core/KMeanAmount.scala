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
  case class UserCluster(cluster:Int, userId:String, amount: Double)
  def props(sc: SparkContext) = Props(new KMeanAmount(sc))
}

class KMeanAmount(sc: SparkContext) extends Actor with ActorLogging {

  import KMeanAmount._

  def receive = {
    case Train(cluster: Int) => trainModel(cluster)
  }

  def splitByString= udf(
    (s : String) =>  {
      // do something with your original column content and return a new one
      if (s == null) null
      else {
        val t = s.split("_")
        if(t.length > 0) t(1)
        else s
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
      .load()

    val userIdDF = transactionDataDF.select("user_id", "original_amount").withColumn("user_id", $"user_id".cast(IntegerType))

    val assembler = new VectorAssembler()
      .setInputCols(Array("user_id", "original_amount"))
      .setOutputCol("features")

    val training = assembler.transform(userIdDF)

    val kmeans = new KMeans().setK(cluster).setSeed(1L)
    val model = kmeans.fit(training)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)

    val transformed = model.transform(training)
//    transformed
//      .select("user_id", "original_amount" , "prediction")
//      .show(false)
    transformed.select("user_id", "original_amount" , "prediction").withColumn("user_id", convertUserId($"user_id")).show(false)
    println(s"Within Set Sum of Squared Errors = $WSSSE")


    //myDF.withColumn("Code", when(myDF("Amt") < 100, "Little").otherwise("Big"))

    val rowRDD = transformed.map(p => UserCluster(p.getAs("prediction"), p.getAs("user_id"), p.getAs("original_amount"))).rdd
    rowRDD.saveToCassandra("events", "userid_by_amount_cluster")
  }
}
