package org.example.application

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("CSV to DataFrame")
      .master("local[*]")
      .getOrCreate()
    println("====================A======================")


    // ٍStep 1: Load CSV file directly
    val schema = StructType(
      Array(
        StructField("Release Year", StringType, nullable = true),
        StructField("Title", StringType, nullable = true),
        StructField("Origin/Ethnicity", StringType, nullable = true),
        StructField("Director", StringType, nullable = true),
        StructField("Cast", StringType, nullable = true),
        StructField("Genre", StringType, nullable = true),
        StructField("Wiki Page", StringType, nullable = true),
        StructField("Plot", StringType, nullable = true)
      )
    )

    val dataFrame = spark.read.schema(schema).option("header", value = true).option("inferSchema", value = false).csv("/Users/filipedasilva/Downloads/wiki_movie_plots_deduped.csv")

    //dataFrame.show()

    def artistHeader(line: String): Array[String] = {
      try {
        var s = line.substring(line.indexOf("Title=\"") + 4)
        val title = s.substring(0, s.indexOf("\""))
        s = s.substring(s.indexOf("Genre=\"") + 5)
        val genre = s.substring(0, s.indexOf("\""))
        s = s.substring(s.indexOf("Plot=\"") + 7)
        val plot = s.substring(0, s.indexOf("\""))
        Array(title, genre, plot)
      } catch {
        case e: Exception => Array("", "", "")
      }
    }



  }
}