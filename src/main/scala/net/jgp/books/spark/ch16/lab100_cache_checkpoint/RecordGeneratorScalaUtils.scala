package net.jgp.books.spark.ch16.lab100_cache_checkpoint

import java.util.{ArrayList, Calendar}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField}

object RecordGeneratorScalaUtils {

  def createDataframe(spark: SparkSession, recordCount: Int): Dataset[Row] = {
    println("-> createDataframe()")
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("name", DataTypes.StringType, false),
      DataTypes.createStructField("title", DataTypes.StringType, false),
      DataTypes.createStructField("rating", DataTypes.IntegerType, false),
      DataTypes.createStructField("year", DataTypes.IntegerType, false),
      DataTypes.createStructField("lang", DataTypes.StringType, false)))

    var df = spark.emptyDataFrame
    val inc = 500000L
    var recordCreated = 0L
    while (recordCreated < recordCount) {
      var recordInc = inc
      if (recordCreated + inc > recordCount)
        recordInc = recordCount - recordCreated
      val rows = new ArrayList[Row]
      for(j <- 0L to recordInc) {
        rows.add(RowFactory.create(getFirstName + " " + getLastName, getTitle, getRating, getRecentYears(25), getLang))
      }
      if (df.isEmpty) df = spark.createDataFrame(rows, schema)
      else df = df.union(spark.createDataFrame(rows, schema))
      recordCreated = df.count()

      println(recordCreated + " records created")
    }
    df.show(3, false)
    System.out.println("<- createDataframe()")
    df
  }

  val cal = Calendar.getInstance

  val fnames = Array("John", "Kevin", "Lydia", "Nathan", "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob",
                    "Mahendra", "Noah", "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam",
                    "Jean-Georges", "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver")

  val lnames = Array("Smith", "Mills", "Perrin", "Foster", "Kumar", "Jones", "Tutt", "Main",
                    "Haque", "Christie", "Karau", "Kahn", "Hahn", "Sanders")

  val articles = Array("The", "My", "A", "Your", "Their")

  val adjectives = Array("", "Great", "Beautiful", "Better", "Worse", "Gorgeous",
                         "Terrific", "Terrible", "Natural", "Wild")


  val nouns = Array("Life", "Trip", "Experience", "Work", "Job", "Beach")

  val lang = Array("fr", "en", "es", "de", "it", "pt")

  val daysInMonth = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  def getLang = lang(getRandomInt(lang.length))

  def getRecentYears(i: Integer): Integer = cal.get(Calendar.YEAR) - getRandomInt(i)

  def getRating: Integer = getRandomInt(3) + 3

  def getRandomInt(i: Int): Int = (Math.random * i).toInt

  def getFirstName = fnames(getRandomInt(fnames.length))

  def getLastName = lnames(getRandomInt(lnames.length))

  def getArticle = articles(getRandomInt(articles.length))

  def getAdjective = adjectives(getRandomInt(adjectives.length))

  def getNoun = nouns(getRandomInt(nouns.length))

  def getTitle: String = (getArticle + " " + getAdjective).trim + " " + getNoun

}
