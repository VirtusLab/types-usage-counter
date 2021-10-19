// using scala 3.0.2
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark

import scala3encoders.given 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}  
import java.util.Base64

 @transient lazy val log = org.apache.log4j.LogManager.getLogger("TASTyJob")

case class SerializedTastyFile(lib: Library, path: String, contentBase64: String):
  def toTastyFile = TastyFile(lib, path, Base64.getDecoder().decode(contentBase64))
  //Base64.getEncoder().encodeToString(bytes)

object SerializedTastyFile:
  def from(tastyFile: TastyFile) =
    SerializedTastyFile(
      tastyFile.lib, 
      tastyFile.path, 
      Base64.getEncoder().encodeToString(tastyFile.content)
    )

case class SerializedTreeInfo(lib: Library, sourceFile: String, 
  method: String, treeKind: String, index: Int, depth: Int, topLevelType: String)

val NoType = "<no-type>"

def processLibraries(libs: Dataset[Library]): Dataset[SerializedTreeInfo] = 
  libs
    .flatMap { lib => 
      val tastyFiles = loadTastyFiles(lib)
      tastyFiles.left.foreach(log.warn)
      tastyFiles.fold(_ => Nil, identity).map(SerializedTastyFile.from) 
    }
    .flatMap { serialized => 
      processTastyFile(serialized.toTastyFile) match
        case util.Left(msg) =>
          log.warn(s"Error occured during TASTy processing: $msg")
          Nil
        case util.Right(infos) =>
          infos.map( info =>
            SerializedTreeInfo(info.lib, info.sourceFile, info.method, info.treeKind, info.index, info.depth, info.topLevelType.getOrElse(NoType))
          )
    }

@main def spark(args: String*) = 
  val (csvPath , limit) = args match 
    case Seq(s) => (s, None)
    case Seq(s, i) => (s, Some(i.toInt))
    case args => throw Exception("Usage: '<csv-file> [<limit>]'")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()
  }

  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val libs = spark.read.option("header", true).csv(csvPath).as[Library]

  val limited = limit.fold(libs)(libs.limit(_))

  val treeInfos = processLibraries(limited.toDF.as[Library])

  val libSizes = treeInfos
    .map {
      case SerializedTreeInfo(lib, _, _, _, _, _, _) => s"${lib.org}:${lib.name.stripSuffix("_3")}"
    }
    .withColumnRenamed("value", "library")
    .groupBy("library")
    .count()
    .sort(col("count").desc)

  libSizes.show(100, false)

  val counts = treeInfos
    .filter(col("topLevelType").=!=(NoType))
    .groupBy("topLevelType")
    .count()
    .sort(col("count").desc)

  counts.show(100, false)

  spark.stop()
