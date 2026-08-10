import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.util.TimeZone
import scala.Ordering.Implicits._

object ProgettoScalable {
    /**
     * Default value of partitions parameter.
     */
    private val defaultPartitions = 8

    implicit val localDateOrdering: Ordering[LocalDate] = _ compareTo _ // https://stackoverflow.com/a/50395814

    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            System.err.println("Missing parameters: <inputFile> [partitions] [workers]")
            return
        }

        val spark = SparkSession.builder
            .appName("Earthquake Application")
            .getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        TimeZone.setDefault(TimeZone.getTimeZone("UTC")) // https://stackoverflow.com/a/52515612

        // Parse parameters
        val filename = args(0)
        var partitions = if (args.length < 2) { defaultPartitions } else { args(1).toInt }
        partitions = partitions * spark.sparkContext.defaultParallelism
        if (args.length > 2) {
            partitions = partitions * args(2).toInt
        }
        println(s"Running with ${partitions} partitions")

        val start = System.currentTimeMillis()

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

        // Partition, remove duplicates and group by date
        val grouped = data
            .partitionBy(new RangePartitioner(partitions, data))
            .distinct()
            .groupByKey()

        // Produce "stripes"
        val mapped = grouped.flatMap(t => {
                val coords = t._2
                for (w <- coords) yield {
                    val map = collection.mutable.Map[(Double, Double), Int]()
                    for {
                        u <- coords
                        if w < u // Avoid counting coordinates twice
                    } {
                        map(u) = map.getOrElseUpdate(u, 0) + 1
                    }
                    (w, map)
                }
            })
            .filter(_._2.nonEmpty)

        def mapCombiner(map1: collection.mutable.Map[(Double, Double), Int], map2: collection.mutable.Map[(Double, Double), Int]) = {
            // aggregateByKey allows to modify and return the first value
            for ((k, v) <- map2) {
                map1(k) = map1.getOrElseUpdate(k, 0) + v
            }
            map1
        }

        // Find most co-occurring coordinates
        val (coords1, (coords2, amount)) = mapped
            // Using aggregateByKey instead of reduceByKey to avoid cloning the maps at each call
            .aggregateByKey(collection.mutable.Map[(Double, Double), Int]())(mapCombiner, mapCombiner)
            .mapValues(_.maxBy(_._2))
            .top(1)(Ordering.by(_._2._2))(0)

        // Find dates with the most co-occurring coordinates
        val dates = grouped
            .flatMapValues(identity)
            .mapValues(t => (t == coords1, t == coords2))
            .filter(t => t._2._1 || t._2._2)
            .reduceByKey {
                case ((c1, c2), (c3, c4)) => (c1 || c3, c2 || c4)
            }
            .filter(_._2 == (true, true))
            .sortByKey()
            .map(_._1)
            .collect()

        assert(dates.length == amount, "Wrong amount of dates, expecting " + amount + " but got " + dates.length)

        println((coords1, coords2))
        dates foreach println

        val elapsed = System.currentTimeMillis() - start
        println(s"Time: ${elapsed / 1000}s")
    }
}