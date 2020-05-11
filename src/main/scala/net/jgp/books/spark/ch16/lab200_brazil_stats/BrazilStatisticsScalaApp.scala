package net.jgp.books.spark.ch16.lab200_brazil_stats

import net.jgp.books.spark.ch16.lab100_cache_checkpoint.Mode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Analytics on Brazil's economy.
  *
  * @author rambabu.posa
  */
object BrazilStatisticsScalaApp {

  /**
    * main() is your entry point to the application.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
                      .appName("Brazil economy")
                      .master("local[*]")
                      .getOrCreate

    spark.sparkContext.setCheckpointDir("/tmp")

    // Reads a CSV file with header, called BRAZIL_CITIES.csv, stores it in
    // a dataframe
    val df = spark.read.format("csv")
                  .option("header", true)
                  .option("sep", ";")
                  .option("enforceSchema", true)
                  .option("nullValue", "null")
                  .option("inferSchema", true)
                  .load("data/brazil/BRAZIL_CITIES.csv")

    println("***** Raw dataset and schema")
    df.show(100)
    df.printSchema()

    // Create and process the records without cache or checkpoint
    val t0 = process(df, NoCacheNoCheckPoint)

    // Create and process the records with cache
    val t1 = process(df, Cache)

    // Create and process the records with a checkpoint
    val t2 = process(df, CheckPoint)

    // Create and process the records with a checkpoint
    val t3 = process(df, CheckPointNonEager)

    println("\n***** Processing times (excluding purification)")
    println("Without cache ............... " + t0 + " ms")
    println("With cache .................. " + t1 + " ms")
    println("With checkpoint ............. " + t2 + " ms")
    println("With non-eager checkpoint ... " + t3 + " ms")

  }

  def process(dataFrame:DataFrame, mode:Mode):Long = {
    val t0 = System.currentTimeMillis

    var df = dataFrame.orderBy(col("CAPITAL").desc)
      .withColumn("WAL-MART", when(col("WAL-MART").isNull, 0).otherwise(col("WAL-MART")))
      .withColumn("MAC", when(col("MAC").isNull, 0).otherwise(col("MAC")))
      .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
      .withColumn("GDP", col("GDP").cast("float"))
      .withColumn("area", regexp_replace(col("area"), ",", ""))
      .withColumn("area", col("area").cast("float"))
      .groupBy("STATE")
      .agg(first("CITY").alias("capital"),
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
      .withColumn("agr_area", expr("agr_area / 100"))
      .orderBy(col("STATE"))
      .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"))

    mode match {
      case Cache =>
        df = df.cache
      case CheckPoint =>
        df = df.checkpoint
      case CheckPointNonEager =>
        df = df.checkpoint(false)
      case _ =>
        df = df
    }

    println("***** Pure data")
    df.show(5)

    val t1 = System.currentTimeMillis
    println("Aggregation (ms) .................. " + (t1 - t0))

    // Regions per population
    println("***** Population")
    val popDf = df.drop("area", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct",
      "mc_donalds_ct", "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita",
      "agr_area", "gdp_2016").orderBy(col("pop_2016").desc)

    popDf.show(30)
    val t2 = System.currentTimeMillis
    println("Population (ms) ................... " + (t2 - t1))

    // Regions per size in km2
    println("***** Area (squared kilometers)")
    val areaDf = df.withColumn("area", round(col("area"), 2))
      .drop("pop_2016", "pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "mc_donalds_ct",
        "agr_area", "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
      .orderBy(col("area").desc)

    areaDf.show(30)
    val t3 = System.currentTimeMillis
    println("Area (ms) ......................... " + (t3 - t2))

    // McDonald's per 1m inhabitants
    println("***** McDonald's restaurants per 1m inhabitants")
    val mcDonaldsPopDf = df.withColumn("mcd_1m_inh", expr("int(mc_donalds_ct / pop_2016 * 100000000) / 100"))
      .drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area",
        "agr_prod", "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
      .orderBy(col("mcd_1m_inh").desc)

    mcDonaldsPopDf.show(5)
    val t4 = System.currentTimeMillis
    println("Mc Donald's (ms) .................. " + (t4 - t3))

    // Walmart per 1m inhabitants
    println("***** Walmart supermarket per 1m inhabitants")
    val walmartPopDf = df.withColumn("walmart_1m_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))
      .drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area", "agr_area", "agr_prod",
        "mc_donalds_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
      .orderBy(col("walmart_1m_inh").desc)

    walmartPopDf.show(5)
    val t5 = System.currentTimeMillis
    println("Walmart (ms) ...................... " + (t5 - t4))

    // GDP per capita
    println("***** GDP per capita")
    val gdpPerCapitaDf = df.drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct", "moto_ct", "area",
      "agr_area", "agr_prod", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area")
      .withColumn("gdp_capita", expr("int(gdp_capita)"))
      .orderBy(col("gdp_capita").desc)

    gdpPerCapitaDf.show(5)
    val t6 = System.currentTimeMillis
    println("GDP per capita (ms) ............... " + (t6 - t5))

    // Post offices
    println("***** Post offices")
    var postOfficeDf = df.withColumn("post_office_1m_inh", expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))
      .withColumn("post_office_100k_km2", expr("int(post_offices_ct / area * 10000000) / 100"))
      .drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "cars_ct", "moto_ct", "agr_area", "agr_prod",
        "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
      .orderBy(col("post_office_1m_inh").desc)

    mode match {
      case Cache =>
        postOfficeDf = postOfficeDf.cache
      case CheckPoint =>
        postOfficeDf = postOfficeDf.checkpoint
      case CheckPointNonEager =>
        postOfficeDf = postOfficeDf.checkpoint(false)
      case _ =>
        postOfficeDf = postOfficeDf
    }

    println("****  Per 1 million inhabitants")
    val postOfficePopDf = postOfficeDf.drop("post_office_100k_km2", "area")
      .orderBy(col("post_office_1m_inh").desc)

    postOfficePopDf.show(5)
    println("****  per 100000 km2")
    val postOfficeArea = postOfficeDf.drop("post_office_1m_inh", "pop_2016")
      .orderBy(col("post_office_100k_km2").desc)

    postOfficeArea.show(5)
    val t7 = System.currentTimeMillis
    println("Post offices (ms) ................. " + (t7 - t6) + " / Mode: " + mode)

    // Cars and motorcycles per 1k habitants
    println("***** Vehicles")
    val vehiclesDf = df.withColumn("veh_1k_inh", expr("int((cars_ct + moto_ct) / pop_2016 * 100000) / 100"))
      .drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "agr_area", "agr_prod",
        "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "area", "pop_brazil")
      .orderBy(col("veh_1k_inh").desc)

    vehiclesDf.show(5)
    val t8 = System.currentTimeMillis
    println("Vehicles (ms) ..................... " + (t8 - t7))

    // Cars and motorcycles per 1k habitants
    println("***** Agriculture - usage of land for agriculture")
    val agricultureDf = df.withColumn("agr_area_pct", expr("int(agr_area / area * 1000) / 10")).withColumn("area", expr("int(area)")).drop("gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita", "post_offices_ct", "moto_ct", "cars_ct", "mc_donalds_ct", "hotels_ct", "beds_ct", "wal_mart_ct", "pop_brazil", "agr_prod", "pop_2016").orderBy(col("agr_area_pct").desc)
    agricultureDf.show(5)
    val t9 = System.currentTimeMillis
    println("Agriculture revenue (ms) .......... " + (t9 - t8))

    val t_ = System.currentTimeMillis
    println("Total with purification (ms) ...... " + (t_ - t0))
    println("Total without purification (ms) ... " + (t_ - t0))

    (t_ - t1)
  }

}
