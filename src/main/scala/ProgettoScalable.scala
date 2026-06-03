import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.util.TimeZone
import scala.Ordering.Implicits._

object ProgettoScalable {
    private val defaultPartitions = 200

    implicit val localDateOrdering: Ordering[LocalDate] = _ compareTo _ // https://stackoverflow.com/a/50395814

    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            System.err.println("Missing parameters: <inputFile> [partitions]")
            return
        }

        val spark = SparkSession.builder
            .appName("Earthquake Application")
            .getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        TimeZone.setDefault(TimeZone.getTimeZone("UTC")) // https://stackoverflow.com/a/52515612

        val filename = args(0)
        val partitions = if (args.length < 2) { defaultPartitions } else { args(1).toInt }

        val rawData = spark.read
            .option("header", value = true)
            // java.lang.Double, java.lang.Double, java.sql.Timestamp
            .schema("longitude DOUBLE, latitude DOUBLE, date TIMESTAMP")
            .csv(filename)
            .rdd

        val data = rawData.map(r => (
            r.getTimestamp(2)
                .toLocalDateTime
                .toLocalDate,
            (Math.round(r.getDouble(1) * 10.0) / 10.0, Math.round(r.getDouble(0) * 10.0) / 10.0)
        ))

        val grouped = data
            .partitionBy(new RangePartitioner(partitions, data))
            .distinct()
            .groupByKey()
            .flatMapValues(identity)

        val (coords, amount) = grouped
            .join(grouped)
            .filter {
                case (_, (c1, c2)) => c1 < c2
            }
            .map(t => (t._2, 1))
            .reduceByKey(_ + _)
            .top(1)(Ordering.by(_._2))(0)

        val dates = grouped
            .mapValues(t => (t == coords._1, t == coords._2))
            .filter(t => t._2._1 || t._2._2)
            .reduceByKey {
                case ((c1, c2), (c3, c4)) => (c1 || c3, c2 || c4)
            }
            .filter(_._2 == (true, true))
            .sortByKey()
            .map(_._1)
            .collect()

        assert(dates.length == amount, "Wrong amount of dates, expecting " + amount + " but got " + dates.length)

        println(coords)
        dates foreach println
    }
}