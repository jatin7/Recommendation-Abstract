package com.infosupport.recommendedcontent.core
import akka.actor.{Actor, ActorLogging, Props}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
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
object KMeanUser {
  case class Train(typeTrain: String, cluster: Int)
  def props(sc: SparkContext) = Props(new KMeanUser(sc))
}

class KMeanUser(sc: SparkContext) extends Actor with ActorLogging {

  import KMeanUser._

  def receive = {
    case Train(typeTrain: String, cluster: Int) => trainModel(typeTrain, cluster)

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
  private def trainModel(typeTrain: String, cluster: Int) = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val transactionDataDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "transaction_data", "keyspace" -> "events"))
      .load()

    import spark.implicits._

    var userIdDF = spark.emptyDataFrame;
    if(typeTrain == "service"){
      userIdDF = transactionDataDF.select("user_id", "service_name", "service_id").withColumn("user_id", $"user_id".cast(IntegerType))
        .withColumn ("service_name",  when($"service_name".isNull,  splitByString($"service_id")  ).otherwise($"service_name") )
    }else{
      userIdDF = transactionDataDF.select("user_id", "service_name", "service_id").withColumn("user_id", $"user_id".cast(IntegerType))
        .withColumn ("service_name",  when($"service_name".isNull,  splitByString($"service_id")  ).otherwise($"service_name") )
    }

    val indexerServiceName = new StringIndexer()
      .setInputCol("service_name")
      .setOutputCol("service_name_index")
      .setHandleInvalid("keep")
    val indexedServiceName = indexerServiceName.fit(userIdDF).transform(userIdDF)

    val assembler = new VectorAssembler()
      .setInputCols(Array("user_id", "service_name_index"))
      .setOutputCol("features")

    val training = assembler.transform(indexedServiceName)

    val kmeans = new KMeans().setK(cluster).setSeed(1L)
    val model = kmeans.fit(training)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training)





    // Shows the result.
    /*println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    val vectors = model.clusterCenters
    println(vectors.mkString("\n"))*/


    // Trains a LDA model.
    /*val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(training)

    val ll = model.logLikelihood(training)
    val lp = model.logPerplexity(training)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)*/


    val transformed = model.transform(training)
    transformed
      .select("user_id", "service_name_index" , "prediction")
      .show(false)

    println(s"Within Set Sum of Squared Errors = $WSSSE")
  }
}

