import org.apache.spark.sql.SparkSession

object ProgettoScalable {
    def main(args: Array[String]): Unit = {
        if (args.length == 0) {
            System.err.println("Missing parameters: <inputFile> [partitions]")
        }

        val spark = SparkSession.builder
            .appName("Earthquake Application")
            .getOrCreate()

        val filename = args(0)
        val data = spark.read.option("header", value = true).csv(filename).rdd


    }
}