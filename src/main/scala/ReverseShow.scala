import org.apache.spark.sql.DataFrame

object ReverseShow extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("ReverseShow")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val solution = showDFToDF(spark.read.text("src\\main\\resources\\show.txt"))
  solution.show()

  def showDFToDF(df: DataFrame): DataFrame = {
    val dfRowsAsArray = df
      .filter(row => row.getString(0).head != '+')
      .withColumn("value", split($"value", "\\|"))

    val colMaxIndex = dfRowsAsArray.head.getSeq[String](0).length - 1
    val colNames = dfRowsAsArray.head.getSeq[String](0).drop(1).dropRight(1).map(_.filter(!_.isWhitespace))

    import org.apache.spark.sql.types.IntegerType
    (1 until colMaxIndex)
      .foldLeft(dfRowsAsArray) { case (frame, i) => frame.withColumn(s"${ colNames(i - 1) }", dfRowsAsArray("value")(i))
      }
      .drop($"value")
      .where(!$"id".contains("id"))
      .withColumn("id", $"id".cast(IntegerType))
  }
}