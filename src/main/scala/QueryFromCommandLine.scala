import scala.sys.exit

object QueryFromCommandLine extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("QueryFromCommandLine")
    .master("local[*]")
    .getOrCreate()

  if (args.length < 2) {
    println("You  must give at least two arguments")
    exit(-1)
  }

  val query = args.tail.mkString(" ")
  val tableName = args.head
  val path = "src\\main\\resources\\" + tableName + ".csv"

  val df = spark.read.option("inferSchema", "true").option("header", "true").csv(path)
  df.createOrReplaceTempView(tableName)
  val solution = spark.sql(query)
  solution.show(truncate = false)
}
