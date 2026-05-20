import org.apache.spark.sql.SparkSession

import java.util.TimeZone

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
            .distinct()

        val first = data
            .groupByKey()
            .flatMap(_._2.toArray.combinations(2).map(a => (a(0), a(1))))
            .filter(coords => coords._1 != coords._2)
            .map((_, 1))
            .reduceByKey(_ + _)
            .sortBy(t => t._2, ascending = false)
            .first()

        println(first)
    }
}