
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SparkTest {


  def main(args: Array[String]): Unit = {


    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      //.config("spark.ui.port","9999")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate();


    val sampleDF = spark.sql(
      """select current_date()""")


    sampleDF.show()


  }

}
