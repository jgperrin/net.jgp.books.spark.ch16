package net.jgp.books.spark.ch16.lab900_books

import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.SparkSession

/**
  * CSV ingestion in a dataframe.
  *
  * @author rambabu.posa
  */
object BasicBookStatsScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Basic book stats")
      .master("local")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    var df = spark.read
      .format("csv")
      .option("header", "true")
      .load("data/goodreads/books.csv")

    df = df.withColumn("main_author", split(col("authors"), "-").getItem(0))

    // Shows at most 5 rows from the dataframe
    df.show(5)

    // Good to stop SparkSession at the end of the application
    spark.stop

  }

}
