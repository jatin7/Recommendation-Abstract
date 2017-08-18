package vn.com.momo.restservice

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.http.scaladsl.settings.RoutingSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import vn.com.momo.core.{KMeanAmount, KMeanCategory, KMeanService, RecommenderSystem}
import org.apache.spark.{SparkConf, SparkContext}
import akka.pattern.ask

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
  * Implementation of the rest service
  */
class RestService(interface: String, port: Int = 3001)(implicit val system: ActorSystem) extends RestServiceProtocol {
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = 30 seconds

  val config = new SparkConf()

  config.setMaster(system.settings.config.getString("spark.master"))
  config.setAppName("recommended-content-service")
  config.set("spark.cassandra.connection.host", system.settings.config.getString("cassandra.server"))

  config.set("spark.driver.memory" , system.settings.config.getString("spark.driver.memory"))
  //config.set("spark.executor.memory", system.settings.config.getString("spark.executor.memory"))
  config.set("spark.cassandra.connection.host", system.settings.config.getString("cassandra.server"))
  config.set("spark.cassandra.auth.username", system.settings.config.getString("cassandra.username"))
  config.set("spark.cassandra.auth.password", system.settings.config.getString("cassandra.password"))

  val sparkContext = new SparkContext(config)

  val recommenderSystem = system.actorOf(RecommenderSystem.props(sparkContext))

  val errorHandler = ExceptionHandler {
    case e: Exception => complete {
      (StatusCodes.InternalServerError -> ErrorResponse("Internal server error"))
    }
  }

  val route = {
    handleExceptions(errorHandler) {
      pathPrefix("recommendations") {
        path(Segment) { id =>
          get {
            complete {
              (recommenderSystem ? RecommenderSystem.GenerateRecommendations(id.toInt))
                .mapTo[RecommenderSystem.Recommendations]
                .flatMap(result => Future {
                  (StatusCodes.OK -> result)
                })
            }
          }
        }
      } ~ path("train") {
        post {
          recommenderSystem ! RecommenderSystem.Train

          complete {
            (StatusCodes.OK -> GenericResponse("Training started"))
          }
        }
      }

//      path("kmean") {
//        post {
//          system.actorOf(KMeanUser.props(sparkContext)) ! KMeanUser.Train()
//
//          complete {
//            (StatusCodes.OK -> GenericResponse("Training started"))
//          }
//        }
//      }

      path("kmeanservice") {
        get {
          parameters('cluster.as[Int], 'iteration.as[Int]) { (cluster, iteration) =>
            complete {
              (system.actorOf(KMeanService.props(sparkContext)) ? KMeanService.Train(cluster, iteration))
              (StatusCodes.OK -> GenericResponse("Training started"))
            }
          }
        }
      }

      path("kmeanamount") {
        get {
          parameters('cluster.as[Int], 'iteration.as[Int]) { (cluster, iteration) =>
            complete {
              (system.actorOf(KMeanAmount.props(sparkContext)) ? KMeanAmount.Train(cluster, iteration))
              (StatusCodes.OK -> GenericResponse("Train started"))
            }
          }
        }
      }

      path("kmeancategory") {
        get {
          parameters('cluster.as[Int], 'iteration.as[Int]) { (cluster, iteration) =>
            complete {
              (system.actorOf(KMeanCategory.props(sparkContext)) ? KMeanCategory.Train(cluster, iteration))
              (StatusCodes.OK -> GenericResponse("Train started"))
            }
          }
        }
      }
    }
  }

  /**
    * Starts the HTTP server
    */
  def start(): Unit = {
    Http().bindAndHandle(route, interface, port)
  }
}
