import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object stream_processing_app {
  def main(args: Array[String]): Unit = {
    println("Stream Processing Application Started")
    val kafka_topic_name = "meetuprsvptopic"
    val kafka_bootstrap_servers = "localhost:9092"

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "<>"
    val mysql_database_name = "meetup_rsvp_db"
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_table_name = "meetup_rsvp_message_agg_detail_table"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val mongodb_host_name = "localhost"
    val mongodb_port_no = "27017"
    //    val mongodb_user_name="pava"
    //    val mongodb_password="<>"
    val mongodb_database_name = "meetuprsvpdb1"
    val mongodb_collection_name = "meetuprsvpcoll1"


    val spark = SparkSession.builder.master("local[*]").appName("Stream Processing Application").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val meetup_rsvp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "latest")
      .load()
    meetup_rsvp_df.printSchema()

    //    println(meetup_rsvp_df.take(1).isEmpty)
    //    println(meetup_rsvp_df.count())

    val meetup_rsvp_message_schema = StructType(Array(
      StructField("venue", StructType(Array(
        StructField("venue_name", StringType),
        StructField("lon", StringType),
        StructField("lat", StringType),
        StructField("venue_id", StringType)
      ))),

      StructField("visibility", StringType),
      StructField("response", StringType),
      StructField("guests", StringType),
      StructField("member", StructType(Array(
        StructField("member_id", StringType),
        StructField("photo", StringType),
        StructField("member_name", StringType)
      ))),

      StructField("rsvp_id", StringType),
      StructField("mtime", StringType),
      StructField("event", StructType(Array(
        StructField("event_name", StringType),
        StructField("event_id", StringType),
        StructField("time", StringType),
        StructField("event_url", StringType)
      ))),
      StructField("group", StructType(Array(
        StructField("group_topics", ArrayType(StructType(Array(
          StructField("urlkey", StringType),
          StructField("topic_name", StringType)
        )), true)),

        StructField("group_city", StringType),
        StructField("group_country", StringType),
        StructField("group_id", StringType),
        StructField("group_name", StringType),
        StructField("group_lon", StringType),
        StructField("group_urlname", StringType),
        StructField("group_lat", StringType)
      )))
    ))

    val meetup_rsvp_df1 = meetup_rsvp_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    val meetup_rsvp_df2 = meetup_rsvp_df1.select(from_json(col("value"), meetup_rsvp_message_schema).as("message_detail"), col("timestamp"))
    val meetup_rsvp_df_3 = meetup_rsvp_df2.select("message_detail.*", "timestamp")
    val meetup_rsvp_df_4 = meetup_rsvp_df_3.select(col("group.group_name"),
      col("group.group_country"), col("group.group_city"),
      col("group.group_lat"), col("group.group_lon"), col("group.group_id"),
      col("group.group_topics"), col("member.member_name"), col("response"),
      col("guests"), col("venue.venue_name"), col("venue.lon"), col("venue.lat"),
      col("venue.venue_id"), col("visibility"), col("member.member_id"),
      col("member.photo"), col("event.event_name"), col("event.event_id"),
      col("event.time"), col("event.event_url")
    )

    println("Printing Schema of meetup_rsvp_df_4")

    //    val tempQuery = meetup_rsvp_df_4.writeStream.outputMode("append").format("console").start()
    //    tempQuery.awaitTermination()

    def processData(batchDF: DataFrame, batchId: Long): Unit = {
      println(batchDF)
      //      val spark_mongodb_output_url= "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":"+mongodb_port_no + "/"+ mongodb_database_name + "." + mongodb_collection_name
      val spark_mongodb_output_url = "mongodb://" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
      println(spark_mongodb_output_url)

      val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))

      var result = batchDF_1.write
        .format("mongo")
        .mode("append")
        .option("uri", spark_mongodb_output_url)
        .option("database", mongodb_database_name)
        .option("collection", mongodb_collection_name)
        .save()
      println(result)
    }

    val tempQuery = meetup_rsvp_df_4.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => processData(batchDF, batchId) }
      .start()

    val meetup_rsvp_df_5 = meetup_rsvp_df_4
      .groupBy("group_name", "group_country", "group_city", "group_lat", "group_lon", "response")
      .agg(count(col("response"))).as("response_count")
    meetup_rsvp_df_5.printSchema()


    val trans_detail_write_stream = meetup_rsvp_df_5.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .option("truncate", "false").format("console").start()

    val mysql_properties = new java.util.Properties
    mysql_properties.setProperty("driver", mysql_driver_class)
    mysql_properties.setProperty("user", mysql_user_name)
    mysql_properties.setProperty("password", mysql_password)

       val tempQuery1=meetup_rsvp_df_5.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        batchDF_1.write.mode("append").jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
      }.start()

    trans_detail_write_stream.awaitTermination()
    tempQuery.awaitTermination()
    tempQuery1.awaitTermination()


      }
  }
