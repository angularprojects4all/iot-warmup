package util

import java.io.File

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import service.SparkMatInteractor
import zio._
import zio.{App, ZEnv, ZIO, console}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object BaseDriver extends zio.App with Logging{
	val name: String = this.getClass.getName
	case class Forecast(sensorId: String, humidity: Int)
	case class Insert(implicit session: SparkSession)
	case class Fetch(implicit session: SparkSession)
	implicit val timeout = Timeout(3.seconds)
	import scala.concurrent.ExecutionContext.Implicits._
	import scala.collection.JavaConversions._

	def run(commandLineArgs: List[String]): URIO[console.Console, ExitCode] = {
		logger info "try giving hadoop.home.dir system property / environment variable / path with winutils.exe folder path"
		System.setProperty("hadoop.home.dir", "C:\\JAVA\\Hadoop")
		val (sparkConf, config) = loadConfig(commandLineArgs.drop(1).headOption)
		lazy val spark = SparkSession.builder()
			.appName(name)
			.master(config.master)
			.config(sparkConf)
			.getOrCreate()
		val startTime = DateTime.now()
		logger.info("Started at " + startTime.toString)

		try{
			run(spark, config, commandLineArgs)
		} catch {
			case e: Exception =>
				logger.error("ERROR", e)
				URIO(ExitCode.failure)

		} finally {
			val endTime = DateTime.now()
			logger.info("Finished at " + endTime.toString() + ", took " + ((endTime.getMillis.toDouble - startTime.getMillis.toDouble) / 1000))
			logger info "cleaning up everything"
			spark.stop
		}
	}

	def run(session: SparkSession, config: AppConfig, params: List[String]) = {
		implicit val sparksession = session
		params.headOption match {
			case Some(str) =>
				val data = Forecast("s1",30)
				val sample = (data.sensorId, data.humidity)
				val sampleResult = Seq(sample, sample)
				val columns = Seq("sensor-id","humidity")
				import session.implicits._
				sampleResult.toDF(columns:_*).write.option("header",true).csv("./spark-warehouse/temps/collections")
			case None =>
				callInsert(5+scala.util.Random.nextInt(3))
				@tailrec
				def callInsert(countDown: Int):Unit = countDown match {
					case x if x > 0 => callInsert(countDown-1)
					case _ => SparkMatInteractor.insertMock(countDown)
				}
				SparkMatInteractor.readMock
		}
		console.putStrLn("Welcome").exitCode
	}

	private def loadConfig(pathArg: Option[String]): (SparkConf, AppConfig) = {
		val config: Config = pathArg.map { path =>
			val file = new File(path)
			if(!file.exists()){
				throw new Exception("Config path " + file.getAbsolutePath + " doesn't exist!")
			}
			logger.info(s"Loading properties from " + file.getAbsolutePath)
			ConfigFactory.load(ConfigFactory.parseFile(file).resolve())
		}.getOrElse {
			ConfigFactory.load(ConfigFactory.parseFile(new File("\\src\\main\\resources\\reference.conf")).resolve())
		}

		val sparkValues = config.entrySet().collect {
			case e if e.getKey.startsWith("spark") => e.getKey -> e.getValue.unwrapped().toString
		}.toMap
		val sparkConf = new SparkConf().setAppName(this.getClass.getName).setAll(sparkValues)

		logger.info("Config")
		(sparkConf.getAll.toSet ++ config.entrySet().map(e => e.getKey -> e.getValue.unwrapped().toString).toSet)
			.toList
			.filterNot(_._1.startsWith("akka."))
			.sortBy(_._1)
			.foreach(kv => logger.info(kv.toString()))

		sparkConf -> new AppConfig(config)
	}

}