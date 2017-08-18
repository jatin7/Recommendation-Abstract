package vn.com.momo.core

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.datastax.spark.connector._

/**
  * Created by giangtrinh on 8/16/17.
  */
object KMeanCategory {
  case class Train(cluster: Int, iteration: Int)
  case class UserCluster(cluster: Int, userId: String, categoryId: Int)
  def props(sc: SparkContext) = Props(new KMeanCategory(sc))
}

class KMeanCategory(sc: SparkContext) extends Actor with ActorLogging {
  import KMeanCategory._

  def receive = {
    case Train(cluster: Int, iteration: Int) => trainModel(cluster, iteration)
  }

  def convertUserId = udf(
    (i: Int) => {
      s"0${i.toString}"
    }
  )

  private def trainModel(cluster: Int, iteration: Int) = {
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    val transactionDataDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> context.system.settings.config.getString("cassandra.database"), "keyspace" -> context.system.settings.config.getString("cassandra.keyspace")))
      .load().cache()

    val dataTrainingDF = transactionDataDF.select("user_id", "category_id")
          .where("category_id IS NOT NULL")
          .withColumn("user_id", $"user_id".cast(IntegerType))
      //.withColumn("category_id_transformed", $"category_id".cast(IntegerType))

    transactionDataDF.unpersist()
    val assembler = new VectorAssembler()
        .setInputCols(Array("user_id", "category_id"))
          .setOutputCol("features")

    val training = assembler.transform(dataTrainingDF)
    training.show()
    val kmeans = new KMeans().setK(cluster).setMaxIter(iteration).setSeed(1L)
    val model = kmeans.fit(training)
    val WSSSE = model.computeCost(training)

    println(s"Within Set Sum of Squared Errors = $WSSSE")
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    val transformed = model.transform(training)
    transformed
      .select("user_id", "category_id" , "prediction")
      .show(false)

    val selectedTranformed = transformed.select("user_id", "category_id" , "prediction").withColumn("user_id", convertUserId($"user_id"))
    val rowRDD = selectedTranformed.map(p => UserCluster(p.getAs("prediction"), p.getAs("user_id"), p.getAs("category_id"))).rdd
    rowRDD.saveToCassandra("events", "userid_by_category_cluster")
  }
}
