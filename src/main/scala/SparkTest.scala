
import org.apache.hadoop.crypto.key.KeyProvider.options
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, concat_ws, lower, regexp_extract, regexp_replace, when}
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.Duration

object SparkTest {


  def main(args: Array[String]): Unit = {


    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)


    val start = LocalDateTime.now

    val my_logger = LoggerFactory.getLogger(getClass.getSimpleName)

    my_logger.info("Spark Session Initiated")

    val spark = SparkSession.builder()
      //.master("local")
      .appName("SparkByExample")
      //.config("spark.ui.port","9999")
      //.config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()


    my_logger.info("Create dataframe from source file")

    val df_raw = spark.read.json("/user/itv000118/stockwits/")

    val df_raw_distinct=df_raw.distinct()

    //df_raw_distinct.coalesce(1).write.format("json")
      //.mode("overwrite")
        //.save("/user/itv000118/deduped2/")

    //my_logger.info("Wrote distinct file")


    val df_final= df_raw_distinct.select(col("created_at"),
      col("id"),
      col("user.username").alias("username"),
      col("entities.sentiment.basic").alias("sentiments"),
      when(regexp_extract(lower(col("body")),"added|adding|started|starter|buying|bought",0)=!="", "Added").otherwise("Null").alias("added"),
      regexp_replace(col("body"),"[\n\r]", " ").alias("message_body"),
      concat_ws(",", col("symbols.symbol")),
      concat_ws(",", col("symbols.title"))
    )


    val df_final_renamed = df_final.withColumnRenamed("concat_ws(,, symbols.symbol)","symbol").withColumnRenamed("concat_ws(,, symbols.title)","title")


    val final_df = df_final_renamed.filter(col("symbol") =!= "")


    val final_df_deduped = final_df.distinct()


    final_df_deduped.show()

    final_df_deduped.write.format("jdbc").
      option("url","jdbc: mysql: //ms.itversity.com:3306/retail_export?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC").
      option("driver","com.mysql.cj.jdbc.Driver").
      option("dbtable" , "tdstk_stock_scala").
      option("user","retail_user").
      option("password","itversity").
      mode("append").
      save()



    my_logger.info("Finished")

    val end = LocalDateTime.now


    val timeElapsed = Duration.between(start, end)

    println("Total time taken is :" + timeElapsed.toMinutes)

  }

}
