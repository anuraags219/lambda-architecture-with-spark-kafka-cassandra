package streaming

import _root_.kafka.serializer.StringDecoder
import domain.{Activity, ActivityByProduct}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import utils.SparkUtils
import functions._
import org.apache.spark.sql.SaveMode

object KafkaStreamingJob extends App {

  val spark = SparkUtils.getSparkSession("Lambda with Spark")

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val batchDuration = Seconds(4)

  val ssc = new StreamingContext(sc, batchDuration)
  val topic: String = "weblogs-text"

  import spark.implicits._

//  val kafkaParams = Map(
//    "zookeeper.connect" -> "localhost:2181",
//    "group.id" -> "lambda",
//    "auto.offset.reset" -> "largest"
//  )

  val kafkaDirectParams = Map(
    "metadata.broker.list" -> "localhost:9092",
    "group.id" -> "lambda",
    "auto.offset.reset" -> "smallest"
  )

  val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaDirectParams, Set(topic)
  )

  val receiverCount = 3

//  val kafkaStreams = (1 to receiverCount).map(_ => {
//    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
//  })

//  val kafkaStream = ssc.union(kafkaStreams)
//    .map(_._2)

//  val kStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//    ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
//    .map(_._2)

  val activityStream = kafkaDirectStream.transform{input =>
    rddToRDDActivity(input)
    }.cache()

  activityStream.foreachRDD{rdd =>
    val activityDF = rdd
      .toDF()
        .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page",
        "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition",
        "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")


    activityDF
      .write
      .partitionBy("topic", "kafkaPartition", "timestamp_hour")
      .mode(SaveMode.Append)
      .csv("hdfs://localhost:9000/user/anuraag/lambda/weblogs-app/")
  }

  val activityStateSpec = StateSpec
    .function(mapActivityStateFunc)
    .timeout(Minutes(120))

  val statefulActivityByProduct = activityStream.transform(rdd => {
    val df = rdd.toDF()
    df.createOrReplaceTempView("activity")
    val activityByProduct = spark.sql(
      """SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)

    activityByProduct
      .map { r =>
        ((r.getString(0), r.getLong(1)),
          ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
        )
      }.rdd
  }).mapWithState(activityStateSpec)

  val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
  activityStateSnapshot
    .reduceByKeyAndWindow(
      (a, b) => b,
      (x, y) => x,
      Seconds(30 / 4 * 4)
    )
    .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
      .toDF().createOrReplaceTempView("ActivityByProduct"))

//  activityStream.foreachRDD(rdd => {
//    val df = rdd.toDF()
//    df.createOrReplaceTempView("activity")
//    spark.sqlContext.cacheTable("activity")
//    df.show(5)
//  })

//  spark.sql("SELECT * from activity")

//  ssc.remember(Minutes(5))

  ssc.start()
  ssc.awaitTermination()

}
