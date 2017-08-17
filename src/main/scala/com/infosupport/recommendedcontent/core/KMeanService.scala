package com.infosupport.recommendedcontent.core
import akka.actor.{Actor, ActorLogging, Props}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.sql.cassandra._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

/**
  * Created by giangtrinh on 8/7/17.
  */
object KMeanService {
  case class Train(cluster: Int, iteration: Int)
  case class UserCluster(cluster:Int, userId:String, serviceName:String)
  def props(sc: SparkContext) = Props(new KMeanService(sc))
}

class KMeanService(sc: SparkContext) extends Actor with ActorLogging {

  import KMeanService._

  def receive = {
    case Train(cluster: Int, iteration: Int) => trainModel(cluster, iteration)
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

  /**
    * Trains the new recommender system model
    */
  private def trainModel(cluster: Int, iteration: Int) = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val transactionDataDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "transaction_data", "keyspace" -> "events"))
      .load()

    import spark.implicits._


    var userIdDF = spark.emptyDataFrame;

    val converterServiceName = new IndexToString()
      .setInputCol("service_name")
      .setOutputCol("origin_service_name")

    val indexerUserId = new StringIndexer()
      .setInputCol("user_id")
      .setOutputCol("user_id_index")
      .setHandleInvalid("keep")
    val indexedUserId = indexerUserId.fit(userIdDF).transform(userIdDF)

    val indexerServiceName = new StringIndexer()
      .setInputCol("service_name")
      .setOutputCol("service_name_index")
      .setHandleInvalid("keep")
    val indexedServiceName = indexerServiceName.fit(indexedUserId).transform(indexedUserId)

    val assembler = new VectorAssembler()
      .setInputCols(Array("user_id_index", "service_name_index"))
      .setOutputCol("features")

    val training = assembler.transform(indexedServiceName)

    val kmeans = new KMeans().setK(cluster).setMaxIter(iteration).setSeed(1L)
    val model = kmeans.fit(training)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)

    val transformed = model.transform(training)
    transformed
      .select("user_id", "service_name_index" , "prediction")
      .show(false)

    println(s"Within Set Sum of Squared Errors = $WSSSE")

    val rowRDD = transformed.map(p => UserCluster(p.getAs("prediction"), p.getAs("user_id"), p.getAs("service_name"))).rdd
    rowRDD.saveToCassandra("events", "userid_by_cluster")
  }
}

