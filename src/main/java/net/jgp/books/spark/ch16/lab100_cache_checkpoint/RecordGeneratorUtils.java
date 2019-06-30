package net.jgp.books.spark.ch16.lab100_cache_checkpoint;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Utility methods to help generate random records.
 * 
 * @author jgp
 */
public abstract class RecordGeneratorUtils {
  private static Calendar cal = Calendar.getInstance();

  private static String[] fnames = { "John", "Kevin", "Lydia", "Nathan",
      "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob", "Mahendra", "Noah",
      "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Jean-Georges",
      "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver" };
  private static String[] lnames = { "Smith", "Mills", "Perrin", "Foster",
      "Kumar", "Jones", "Tutt", "Main", "Haque", "Christie", "Karau",
      "Kahn", "Hahn", "Sanders" };
  private static String[] articles = { "The", "My", "A", "Your", "Their" };
  private static String[] adjectives = { "", "Great", "Beautiful", "Better",
      "Worse", "Gorgeous", "Terrific", "Terrible", "Natural", "Wild" };
  private static String[] nouns = { "Life", "Trip", "Experience", "Work",
      "Job", "Beach" };
  private static String[] lang = { "fr", "en", "es", "de", "it", "pt" };
  private static int[] daysInMonth =
      { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

  public static Dataset<Row> createDataframe(SparkSession spark,
      int recordCount) {
    System.out.println("-> createDataframe()");
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "name",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "title",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "rating",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "year",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "lang",
            DataTypes.StringType,
            false) });

    Dataset<Row> df = null;
    int inc = 500000;
    long recordCreated = 0;
    while (recordCreated < recordCount) {
      long recordInc = inc;
      if (recordCreated + inc > recordCount) {
        recordInc = recordCount - recordCreated;
      }
      List<Row> rows = new ArrayList<>();
      for (long j = 0; j < recordInc; j++) {
        rows.add(RowFactory.create(
            getFirstName() + " " + getLastName(),
            getTitle(),
            getRating(),
            getRecentYears(25),
            getLang()));
      }
      if (df == null) {
        df = spark.createDataFrame(rows, schema);
      } else {
        df = df.union(spark.createDataFrame(rows, schema));
      }
      recordCreated = df.count();
      System.out.println(recordCreated + " records created");
    }

    df.show(3, false);
    System.out.println("<- createDataframe()");
    return df;
  }

  public static String getLang() {
    return lang[getRandomInt(lang.length)];
  }

  public static int getRecentYears(int i) {
    return cal.get(Calendar.YEAR) - getRandomInt(i);
  }

  public static int getRating() {
    return getRandomInt(3) + 3;
  }

  public static String getRandomSSN() {
    return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-"
        + getRandomInt(10) + getRandomInt(10)
        + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)
        + getRandomInt(10);
  }

  public static int getRandomInt(int i) {
    return (int) (Math.random() * i);
  }

  public static String getFirstName() {
    return fnames[getRandomInt(fnames.length)];
  }

  public static String getLastName() {
    return lnames[getRandomInt(lnames.length)];
  }

  public static String getArticle() {
    return articles[getRandomInt(articles.length)];
  }

  public static String getAdjective() {
    return adjectives[getRandomInt(adjectives.length)];
  }

  public static String getNoun() {
    return nouns[getRandomInt(nouns.length)];
  }

  public static String getTitle() {
    return (getArticle() + " " + getAdjective()).trim() + " " + getNoun();
  }

  public static int getIdentifier(List<Integer> identifiers) {
    int i;
    do {
      i = getRandomInt(60000);
    } while (identifiers.contains(i));

    return i;
  }

  public static Integer
      getLinkedIdentifier(List<Integer> linkedIdentifiers) {
    if (linkedIdentifiers == null) {
      return -1;
    }
    if (linkedIdentifiers.isEmpty()) {
      return -2;
    }
    int i = getRandomInt(linkedIdentifiers.size());
    return linkedIdentifiers.get(i);
  }

  public static String getLivingPersonDateOfBirth(String format) {
    int year = cal.get(Calendar.YEAR) - getRandomInt(120) + 15;
    int month = getRandomInt(12);
    int day = getRandomInt(daysInMonth[month]) + 1;
    cal.set(year, month, day);

    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(cal.getTime());
  }

}
