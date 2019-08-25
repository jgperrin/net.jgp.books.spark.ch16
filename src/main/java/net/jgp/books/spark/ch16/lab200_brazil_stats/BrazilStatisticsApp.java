package net.jgp.books.spark.ch16.lab200_brazil_stats;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analytics on Brazil's economy.
 * 
 * @author jgp
 */
public class BrazilStatisticsApp {
  private static Logger log =
      LoggerFactory.getLogger(BrazilStatisticsApp.class);

  enum Mode {
    NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER
  }

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
        .appName("Brazil economy")
        .master("local[*]")
        .getOrCreate();
    SparkContext sc = spark.sparkContext();
    sc.setCheckpointDir("/tmp");

    StringBuilder report = new StringBuilder();

    // Reads a CSV file with header, called BRAZIL_CITIES.csv, stores it in
    // a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("sep", ";")
        .option("enforceSchema", true)
        .option("nullValue", "null")
        .option("inferSchema", true)
        .load("data/brazil/BRAZIL_CITIES.csv");
    System.out.println("***** Raw dataset and schema");
    df.show(100);
    df.printSchema();

    // Create and process the records without cache or checkpoint
    long t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT);

    // Create and process the records with cache
    long t1 = process(df, Mode.CACHE);

    // Create and process the records with a checkpoint
    long t2 = process(df, Mode.CHECKPOINT);

    // Create and process the records with a checkpoint
    long t3 = process(df, Mode.CHECKPOINT_NON_EAGER);

    System.out.println("\n***** Processing times (excluding purification)");
    System.out.println("Without cache ............... " + t0 + " ms");
    System.out.println("With cache .................. " + t1 + " ms");
    System.out.println("With checkpoint ............. " + t2 + " ms");
    System.out.println("With non-eager checkpoint ... " + t3 + " ms");
  }

  long process(Dataset<Row> df, Mode mode) {
    long t0 = System.currentTimeMillis();

    df = df
        .orderBy(col("CAPITAL").desc())
        .withColumn("WAL-MART",
            when(col("WAL-MART").isNull(), 0).otherwise(col("WAL-MART")))
        .withColumn("MAC",
            when(col("MAC").isNull(), 0).otherwise(col("MAC")))
        .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
        .withColumn("GDP", col("GDP").cast("float"))
        .withColumn("area", regexp_replace(col("area"), ",", ""))
        .withColumn("area", col("area").cast("float"))
        .groupBy("STATE")
        .agg(
            first("CITY").alias("capital"),
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
        .withColumn("agr_area", expr("agr_area / 100")) // converts hectares
                                                        // to km2
        .orderBy(col("STATE"))
        .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"));
    switch (mode) {
      case CACHE:
        df = df.cache();
        break;

      case CHECKPOINT:
        df = df.checkpoint();
        break;

      case CHECKPOINT_NON_EAGER:
        df = df.checkpoint(false);
        break;
    }
    System.out.println("***** Pure data");
    df.show(5);

    long t1 = System.currentTimeMillis();
    System.out.println("Aggregation (ms) .................. " + (t1 - t0));

    // Regions per population
    System.out.println("***** Population");
    Dataset<Row> popDf = df
        .drop(
            "area", "pop_brazil", "pop_foreign", "post_offices_ct",
            "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
            "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
            "gdp_2016")
        .orderBy(col("pop_2016").desc());
    popDf.show(30);
    long t2 = System.currentTimeMillis();
    System.out.println("Population (ms) ................... " + (t2 - t1));

    // Regions per size in km2
    System.out.println("***** Area (squared kilometers)");
    Dataset<Row> areaDf = df
        .withColumn("area", round(col("area"), 2))
        .drop(
            "pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct",
            "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
            "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
            "gdp_2016")
        .orderBy(col("area").desc());
    areaDf.show(30);
    long t3 = System.currentTimeMillis();
    System.out.println("Area (ms) ......................... " + (t3 - t2));

    // McDonald's per 1m inhabitants
    System.out.println("***** McDonald's restaurants per 1m inhabitants");
    Dataset<Row> mcDonaldsPopDf = df
        .withColumn("mcd_1m_inh",
            expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100"))
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "wal_mart_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
        .orderBy(col("mcd_1m_inh").desc());
    mcDonaldsPopDf.show(5);
    long t4 = System.currentTimeMillis();
    System.out.println("Mc Donald's (ms) .................. " + (t4 - t3));

    // Walmart per 1m inhabitants
    System.out.println("***** Walmart supermarket per 1m inhabitants");
    Dataset<Row> walmartPopDf = df
        .withColumn("walmart_1m_inh",
            expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
        .orderBy(col("walmart_1m_inh").desc());
    walmartPopDf.show(5);
    long t5 = System.currentTimeMillis();
    System.out.println("Walmart (ms) ...................... " + (t5 - t4));

    // GDP per capita
    System.out.println("***** GDP per capita");
    Dataset<Row> gdpPerCapitaDf = df
        .drop(
            "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
            "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area")
        .withColumn("gdp_capita", expr("int(gdp_capita)"))
        .orderBy(col("gdp_capita").desc());
    gdpPerCapitaDf.show(5);
    long t6 = System.currentTimeMillis();
    System.out.println("GDP per capita (ms) ............... " + (t6 - t5));

    // Post offices
    System.out.println("***** Post offices");
    Dataset<Row> postOfficeDf = df
        .withColumn("post_office_1m_inh",
            expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))
        .withColumn("post_office_100k_km2",
            expr("int(post_offices_ct / area * 10000000) / 100"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
        .orderBy(col("post_office_1m_inh").desc());
    switch (mode) {
      case CACHE:
        postOfficeDf = postOfficeDf.cache();
        break;
      case CHECKPOINT:
        postOfficeDf = postOfficeDf.checkpoint();
        break;
      case CHECKPOINT_NON_EAGER:
        postOfficeDf = postOfficeDf.checkpoint(false);
        break;
    }
    System.out.println("****  Per 1 million inhabitants");
    Dataset<Row> postOfficePopDf = postOfficeDf
        .drop("post_office_100k_km2", "area")
        .orderBy(col("post_office_1m_inh").desc());
    postOfficePopDf.show(5);
    System.out.println("****  per 100000 km2");
    Dataset<Row> postOfficeArea = postOfficeDf
        .drop("post_office_1m_inh", "pop_2016")
        .orderBy(col("post_office_100k_km2").desc());
    postOfficeArea.show(5);
    long t7 = System.currentTimeMillis();
    System.out.println(
        "Post offices (ms) ................. " + (t7 - t6) + " / Mode: "
            + mode);

    // Cars and motorcycles per 1k habitants
    System.out.println("***** Vehicles");
    Dataset<Row> vehiclesDf = df
        .withColumn("veh_1k_inh",
            expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "agr_area", "agr_prod", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area",
            "pop_brazil")
        .orderBy(col("veh_1k_inh").desc());
    vehiclesDf.show(5);
    long t8 = System.currentTimeMillis();
    System.out.println("Vehicles (ms) ..................... " + (t8 - t7));

    // Cars and motorcycles per 1k habitants
    System.out.println("***** Agriculture - usage of land for agriculture");
    Dataset<Row> agricultureDf = df
        .withColumn("agr_area_pct",
            expr("int(agr_area / area * 1000) / 10"))
        .withColumn("area",
            expr("int(area)"))
        .drop(
            "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
            "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct",
            "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod",
            "pop_2016")
        .orderBy(col("agr_area_pct").desc());
    agricultureDf.show(5);
    long t9 = System.currentTimeMillis();
    System.out.println("Agriculture revenue (ms) .......... " + (t9 - t8));

    long t_ = System.currentTimeMillis();
    System.out.println("Total with purification (ms) ...... " + (t_ - t0));
    System.out.println("Total without purification (ms) ... " + (t_ - t0));

    return t_ - t1;
  }
}
