import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
object spark_demo {
  def main(args: Array[String]): Unit = {
    println("Hello_world")

    //Create a spark session
    val spark=SparkSession
      .builder()
      .appName(name = "HelloSpark")
      .master(master = "local[*]")
      .getOrCreate()
    println("Hello world!! My first scala program")
    val sampleSeq= Seq((1,"spark"), (2,"bigdata"))
    val df=spark.createDataFrame(sampleSeq).toDF("Value","Remarks")
    df.show(false)
    val df2=df.withColumn("Index", lit("null"))
    df2.show(false)
  }

}
