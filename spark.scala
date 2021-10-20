// using scala 3.0.2
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark

import scala3encoders.given

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import java.util.Base64

@transient lazy val log = org.apache.log4j.LogManager.getLogger("TASTyJob")

case class SerializedTastyFile(
    lib: Library,
    path: String,
    contentBase64: String
):
  def toTastyFile =
    TastyFile(lib, path, Base64.getDecoder().decode(contentBase64))

object SerializedTastyFile:
  def from(tastyFile: TastyFile) =
    val encoded = Base64.getEncoder().encodeToString(tastyFile.content)
    SerializedTastyFile(tastyFile.lib, tastyFile.path, encoded)

case class SerializedTreeInfo(
    lib: Library,
    sourceFile: String,
    method: String,
    treeKind: String,
    index: Int,
    depth: Int,
    topLevelType: String
)

val NoType = "<no-type>"

object SerializedTreeInfo:
  def from(info: TreeInfo) =
    SerializedTreeInfo(
      info.lib,
      info.sourceFile,
      info.method,
      info.treeKind,
      info.index,
      info.depth,
      info.topLevelType.getOrElse(NoType)
    )

def processLibraries(libs: Dataset[Library]): Dataset[SerializedTreeInfo] =
  libs
    .flatMap { lib =>
      val tastyFiles: Either[String, Seq[TastyFile]] = loadTastyFiles(lib)
      tastyFiles.left.foreach(log.warn)
      tastyFiles.toSeq.flatten.map(SerializedTastyFile.from)
    }
    .flatMap { serialized =>
      val treeInfos: Either[String, Seq[TreeInfo]] =
        processTastyFile(serialized.toTastyFile)
      treeInfos.left.foreach(log.warn)
      treeInfos.toSeq.flatten.map(SerializedTreeInfo.from)
    }

@main def spark(args: String*) =
  val csvPath = args.headOption.getOrElse("libs.csv")

  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  val libs = spark.read.option("header", true).csv(csvPath).as[Library]
  val treeInfos = processLibraries(libs)

  treeInfos
    .filter(col("topLevelType").=!=(NoType))
    .groupBy("topLevelType")
    .count()
    .sort(col("count").desc)
    .show(10, false)

  spark.stop()
