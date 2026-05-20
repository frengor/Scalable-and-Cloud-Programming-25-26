import org.apache.spark.sql.SparkSession

import java.util.TimeZone
import scala.Ordering.Implicits._

object ProgettoScalable {
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

        val first = data
            .groupByKey()
            .flatMap(
                _._2.toArray
                    .combinations(2)
                    .filter {
                        case Array(c1, c2) => c1 != c2
                    }
                    .map {
                        case Array(c1, c2) => if (c1 < c2) {
                            (c1, c2)
                        } else {
                            (c2, c1)
                        }
                    }
            )
            .map((_, 1))
            .reduceByKey(_ + _)
            .sortBy(t => t._2, ascending = false)
            .first()

        println(first)
    }
}