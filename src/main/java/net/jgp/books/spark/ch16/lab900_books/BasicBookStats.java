package net.jgp.books.spark.ch16.lab900_books;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class BasicBookStats {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    BasicBookStats app = new BasicBookStats();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Basic book stats")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load("data/goodreads/books.csv");

    df = df
        .withColumn("main_author", split(col("authors"), "-").getItem(0));

    // Shows at most 5 rows from the dataframe
    df.show(5);
  }
}
