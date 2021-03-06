package batch

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import domain._
import utils.SparkUtils

object BatchJob {

  def main(args: Array[String]) : Unit = {

    // setup spark context

    val spark = SparkUtils.getSparkSession("Lambda with Spark")
    val sc = spark.sparkContext

    // check if running from IDE
//    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
//      conf.setMaster("local[*]")
//    }

    sc.setLogLevel("ERROR")

    import spark.implicits._

    // initialize input RDD
    val sourceFile = "file:///home/anuraag/Downloads/Applying the Lambda Architecture/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap(line => {
      val record = line.split("\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if(record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else None
    }).toDF()

    val df = inputDF.select(
      add_months(from_unixtime($"timestamp_hour" / 1000), 1).as("timestamp_hour"),
      $"referrer", $"action", $"prevPage", $"page", $"visitor", $"product"
    )

    df.createOrReplaceTempView("activity")

    val visitorsByProduct = spark.sql("""SELECT
                                        |product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
                                        |FROM activity
                                        |GROUP BY product, timestamp_hour
                                        |""".stripMargin)

    val activityByProduct = spark.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    activityByProduct.write
      .partitionBy("timestamp_hour")
      .mode(SaveMode.Append)
      .parquet("hdfs://localhost:9000/user/anuraag/lambda/batch1")

    visitorsByProduct.show(10)
    activityByProduct.show(10)

  }

}
