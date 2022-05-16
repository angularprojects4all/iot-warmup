package service

import org.apache.spark.sql.SparkSession
import service.SparkMatInteractor.{insertMock, readMock}
import util.BaseDriver.{Fetch, Forecast, Insert}

trait SparkInteractor {
  def insertMock(passes: Int)(implicit s: SparkSession)
  def insertSample(implicit s: SparkSession)
  def readMock(implicit s: SparkSession)
  def readSample(implicit s: SparkSession)
}

object SparkMatInteractor extends SparkInteractor {
  override def insertMock(passes: Int)(implicit session: SparkSession): Unit = passes match {
    case x if x > 0 => insertMock(passes-1)
    case _ =>
      val columns = Seq("sensor-id","humidity")
      val sensors = (1 until (10000 + scala.util.Random.nextInt(10000))).map {
        id => "s"+id.toString
      }.toList
      val sampleResult: List[(String,String)] = sensors.map(entry => {
        if(entry.split("s")(1).toInt % 100 == 0) (entry,"")
        else (entry, scala.util.Random.nextInt(100)+"")
      }).toList
      import session.implicits._
      sampleResult.toDF(columns:_*).write.option("header",true).mode("append").csv("./spark-warehouse/temps/collections")
  }

  override def insertSample(implicit s: SparkSession): Unit = ???

  override def readMock(implicit spark: SparkSession): Unit = {
    val df = spark.read.option("header","true").option("mode", "PERMISSIVE") .csv("./spark-warehouse/temps/collections")
    df.groupBy("sensor-id").agg("humidity" -> "min", "humidity" -> "avg", "humidity" -> "max").orderBy(org.apache.spark.sql.functions.col("avg(humidity)").desc ).show(1000)
  }

  override def readSample(implicit s: SparkSession): Unit = ???
}