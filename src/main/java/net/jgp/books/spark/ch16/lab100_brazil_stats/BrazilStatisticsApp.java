package net.jgp.books.spark.ch16.lab100_brazil_stats;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "CITY",
            DataTypes.StringType,
            true),
        DataTypes.createStructField(
            "STATE",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "CAPITAL",
            DataTypes.BooleanType,
            false),
        DataTypes.createStructField(
            "IBGE_RES_POP",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_RES_POP_BRAS",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_RES_POP_ESTR",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_DU",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_DU_URBAN",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_DU_RURAL",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_POP",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_1",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_1-4",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_5-9",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_10-14",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_15-59",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_60+",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "IBGE_PLANTED_AREA",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "IBGE_CROP_PRODUCTION_$",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "IDHM Ranking 2010",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "IDHM",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "IDHM_Renda",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "IDHM_Longevidade",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "IDHM_Educacao",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "LONG",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "LAT",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "ALT",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "PAY_TV",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "FIXED_PHONES",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "AREA",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "REGIAO_TUR",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "CATEGORIA_TUR",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "ESTIMATED_POP",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "RURAL_URBAN",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "GVA_AGROPEC",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "GVA_INDUSTRY",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "GVA_SERVICES",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "GVA_PUBLIC",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            " GVA_TOTAL ",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "TAXES",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "GDP",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "POP_GDP",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "GDP_CAPITA",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "GVA_MAIN",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "MUN_EXPENDIT",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "COMP_TOT",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_A",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_B",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_C",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_D",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_E",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_F",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_G",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_H",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_I",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_J",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_K",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_L",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_M",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_N",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_O",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_P",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_Q",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_R",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_S",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_T",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "COMP_U",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "HOTELS",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "BEDS",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Pr_Agencies",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Pu_Agencies",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Pr_Bank",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Pu_Bank",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Pr_Assets",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "Pu_Assets",
            DataTypes.LongType,
            false),
        DataTypes.createStructField(
            "Cars",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Motorcycles",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "Wheeled_tractor",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "UBER",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "MAC",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "WAL-MART",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "POST_OFFICES",
            DataTypes.IntegerType,
            false) });

    // Reads a CSV file with header, called BRAZIL_CITIES.csv, stores it in
    // a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("sep", ";")
        .option("enforceSchema", true)
        // .option("nullValue", "null")
        // .option("inferSchema", true)
        .schema(schema)
        .load("data/brazil/BRAZIL_CITIES.csv");
    df.show(10);
    df.printSchema();

    long t0 = System.currentTimeMillis();

    df = df
        .orderBy(col("CAPITAL").desc())
        .withColumn("GDP_corrected", when(col("GDP").isNull(), lit(0)))
        .groupBy("STATE")
        .agg(
            first("CITY").alias("city"),
            sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
            sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
            sum("POP_GDP").alias("pop_2016"),
            sum("GDP_corrected").alias("gdb_2016"),
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
        .withColumn("gdp_capita", expr("gdb_2016 / pop_2016"));

    long t1 = System.currentTimeMillis();
    System.out.println("Aggregation (ms) ... " + (t1 - t0));
    df.show(40);

  }
}
