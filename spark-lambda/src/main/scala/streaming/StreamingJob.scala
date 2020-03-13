package streaming

import config.Settings
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Minutes, Seconds, State, StateSpec, StreamingContext}
import utils.SparkUtils
import functions._
import com.twitter.algebird.HyperLogLogMonoid

object StreamingJob {

  def main(args: Array[String]) : Unit = {
    val spark = SparkUtils.getSparkSession("Lambda with Spark")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val batchDuration = Seconds(4)

    import spark.implicits._

    def streamingApp(sc: SparkContext, batchDuration: Duration) : StreamingContext = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = Settings.WebLogGen.destPath

      val textDStream = ssc.textFileStream(inputPath)
      val activityStream = textDStream.transform(input => {
        input.flatMap { line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      }).cache()

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

      // unique visitors by product

      val visitorStateSpec = StateSpec
        .function(mapVisitorsStateSpec)
        .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map(a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
          .reduceByKeyAndWindow(
            (a, b) => b,
            (x, y) => x,
            Seconds(30 / 4 * 4))
          .foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
          .toDF().createOrReplaceTempView("VisitorsByProduct"))


//        .updateStateByKey((newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]) => {
//        var (prevTimestamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((System.currentTimeMillis(), 0L, 0L, 0L))
//        var result : Option[(Long, Long, Long, Long)] = null
//
//        if(newItemsPerKey.isEmpty) {
//          if(System.currentTimeMillis() - prevTimestamp > 30000 + 4000)
//            result = None
//          else
//            result = Some(prevTimestamp, purchase_count, add_to_cart_count, page_view_count)
//        } else {
//          newItemsPerKey.foreach(a => {
//            purchase_count += a.purchase_count
//            add_to_cart_count += a.add_to_cart_count
//            page_view_count += a.page_view_count
//          })
//
//          result = Some(System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count)
//        }
//
//        result
//      })
//
//      statefulActivityByProduct.print(10)
      ssc
    }

    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()

  }

}
