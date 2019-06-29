package net.jgp.books.spark.ch16.lab200_brazil_stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orders analytics.
 * 
 * @author jgp
 */
public class BrazilStatisticsApp {
  private static Logger log =
      LoggerFactory.getLogger(BrazilStatisticsApp.class);

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    BrazilStatisticsApp app = new BrazilStatisticsApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Brazilian municipalities and states")
        .master("local[*]")
        .getOrCreate();
    SparkContext sc = spark.sparkContext();
    sc.setCheckpointDir("/tmp");
    
    // Reads a CSV file with header, called BRAZIL_CITIES.csv, stores it in
    // a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("sep", ";")
        .option("enforceSchema", true)
        .option("nullValue", "null")
        .option("inferSchema", true)
        .load("data/brazil/BRAZIL_CITIES.csv");
    df.show(100);
    df.printSchema();

    long t0 = System.currentTimeMillis();

    df = df
        .orderBy(col("CAPITAL").desc())
        .withColumn("WAL-MART",
            when(col("WAL-MART").isNull(), 0).otherwise(col("WAL-MART")))
        .withColumn("MAC",
            when(col("MAC").isNull(), 0).otherwise(col("MAC")))
        .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
        .withColumn("GDP", col("GDP").cast("float"))
        .withColumn("area", regexp_replace(col("area"), ",", "."))
        .withColumn("area", col("area").cast("float"))
        .groupBy("STATE")
        .agg(
            first("CITY").alias("city"),
            sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
            sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
            sum("POP_GDP").alias("pop_2016"),
            sum("GDP").alias("gdp_2016"),
            sum("POST_OFFICES").alias("post_offices_ct"),
            sum("WAL-MART").alias("wal_mart_ct"),
            sum("MAC").alias("mc_donalds_ct"),
            sum("Cars").alias("cars_ct"),
            sum("Motorcycles").alias("moto_ct"),
            sum("AREA").alias("area"),
            sum("IBGE_PLANTED_AREA").alias("agr_area"),
            sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
            sum("HOTELS").alias("hotels_ct"),
            sum("BEDS").alias("beds_ct"))
        .orderBy(col("STATE"))
        .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"))
        ;//.checkpoint(false);
    df.show(5);

    long t1 = System.currentTimeMillis();
    System.out.println("Aggregation (ms) ............. " + (t1 - t0));

    // Mc Donald's per 1m inhabitants
    Dataset<Row> mcDonaldsPopDf = df
        .withColumn("mcd_1m_inh",
            expr("mc_donalds_ct / pop_2016 * 1000000"))
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
        .orderBy(col("mcd_1m_inh").desc());
    mcDonaldsPopDf.show(5);

    long t2 = System.currentTimeMillis();
    System.out.println("Mc Donald's (ms) ............. " + (t2 - t1));

    // Walmart per 1m inhabitants
    Dataset<Row> walmartPopDf = df
        .withColumn("walmart_1m_inh",
            expr("wal_mart_ct / pop_2016 * 1000000"))
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
        .orderBy(col("walmart_1m_inh").desc());
    walmartPopDf.show(5);

    long t3 = System.currentTimeMillis();
    System.out.println("Walmart (ms) ................. " + (t3 - t2));

    // GDP per capita
    Dataset<Row> gdpPerCapitaDf = df
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area")
        .orderBy(col("gdp_capita").desc());
    gdpPerCapitaDf.show(5);
    long t4 = System.currentTimeMillis();
    System.out.println("GDP per capita (ms) ... " + (t4 - t3));

    // GDP per capita
    Dataset<Row> postOfficeDf = df
        .withColumn("post_office_1m_inh",
            expr("post_offices_ct / pop_2016 * 1000000"))
        .withColumn("post_office_1k_km2",
            expr("post_offices_ct / area * 1000"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
        .orderBy(col("post_office_1m_inh").desc());
    postOfficeDf.show(5);
    postOfficeDf = postOfficeDf.orderBy(col("post_office_1k_km2").desc());
    postOfficeDf.show(5);
    long t5 = System.currentTimeMillis();
    System.out.println("Post offices (ms) ............ " + (t5 - t4));

    // Cars and motorcycles per 1k habitants
    Dataset<Row> vehiclesDf = df
        .withColumn("veh_1k_inh",
            expr("(cars_ct + moto_ct) / pop_2016 * 1000"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area",
            "pop_brazil")
        .orderBy(col("veh_1k_inh").desc());
    vehiclesDf.show(5);
    long t6 = System.currentTimeMillis();
    System.out.println("Post offices (ms) ............ " + (t6 - t5));

    // Cars and motorcycles per 1k habitants
    Dataset<Row> agricultureDf = df
        .withColumn("agr_revenue",
            expr("agr_prod / agr_area"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "area",
            "pop_2016")
        .orderBy(col("agr_revenue").desc());
    agricultureDf.show(5);
    long t7 = System.currentTimeMillis();
    System.out.println("Agriculture revenue (ms) ..... " + (t7 - t6));

    long t_ = System.currentTimeMillis();
    System.out.println("Total (ms) ................... " + (t_ - t0));
  }
}
