package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkUtils {

  val checkpointDirectory = "hdfs://localhost:9000/user/anuraag/lambda/checkpoint"

  def getSparkSession(appName: String) : SparkSession = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setCheckpointDir(checkpointDirectory)
    spark

  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) : StreamingContext= {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc

  }

}
