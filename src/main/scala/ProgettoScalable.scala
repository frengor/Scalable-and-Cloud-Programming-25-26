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
        val data = spark.read.option("header", value = true).schema("longitude DOUBLE, latitude DOUBLE, date TIMESTAMP").csv(filename).rdd

        // java.lang.Double, java.lang.Double, java.sql.Timestamp
        println(data.collect().toSeq.map(r => r.toSeq.map(e => e.getClass.getName)).head)
    }
}