object MostImportantRows extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("MostImportantRows")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val priorities = Seq(("MV1", 1), ("MV2", 2), ("VPV",3), ("Others", 4)).toDF("value", "rank")
  val input = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others"),
    (3, "MV1"),
    (3, "MV2"),
    (3, "VPV"),
    (3, "Others")).toDF("id", "value")

  val solution = input
    .join(priorities, "value")
    .groupBy($"id")
    .agg(min($"rank").as("rank"))
    .join(priorities, "rank")
    .drop($"rank")
  solution.show()

}