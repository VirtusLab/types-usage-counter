// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using lib tasty-query::tasty-query:0.0.2-spark

import java.io.File

import util.Try
import util.Using

import tastyquery.reader.TastyUnpickler
import tastyquery.Contexts
import tastyquery.api.ProjectReader
import tastyquery.ast.Trees.*
import tastyquery.ast.Types.*
import tastyquery.ast.Names.Name

import java.util.zip.ZipInputStream
import java.io.ByteArrayInputStream
import java.net.URL
import java.io.ByteArrayOutputStream
import javax.naming.spi.DirStateFactory.Result
import java.io.InputStream

case class Library(org: String, name: String, version: String):
  def moduleString = s"$org:${name.stripSuffix("_3")}"

// path is relative to the jar, e.g. 'foo/Bar.tasty' for a `foo.Bar` class
case class TastyFile(lib: Library, path: String, content: Array[Byte])
case class TreeInfo(
    lib: Library,
    sourceFile: String, // path to tasty file
    method: String, // name of top level method, class, object etc. that contains that tree
    treeKind: String, // type of the tree, e.g. Select or Apply
    index: Int, // index from the top of the tree
    depth: Int, // how much given tree is nested
    topLevelType: Option[String] // top level type, excluding generic, e.g. Option[List[Int]] becomes just scala.Option
)

// Loads library from maven central and exposed input stream
def loadLibrary(lib: Library): Either[String, InputStream] =
  val orgPath = lib.org.split('.').mkString("/")
  val repo = "https://repo1.maven.org/maven2"
  val address =
    s"$repo/$orgPath/${lib.name}/${lib.version}/${lib.name}-${lib.version}.jar"
  try Right(URL(address).openStream())
  catch
    case e: java.io.FileNotFoundException =>
      Left(s"Artifact not present: $address")

// Load given library jar and extract tasty files to case classes
// Works in memory
def loadTastyFiles(lib: Library): Either[String, Seq[TastyFile]] =
  loadLibrary(lib).flatMap { urlIs =>
    try
      val zipIs = ZipInputStream(urlIs)
      val tastyEntries = LazyList
        .continually(zipIs.getNextEntry)
        .takeWhile(_ != null)
        .filter(_.getName.endsWith(".tasty"))

      val tastyFiles = tastyEntries.map { entry =>
        val out = ByteArrayOutputStream()
        val buffer = new Array[Byte](4096)
        LazyList
          .continually(zipIs.read(buffer))
          .takeWhile(_ != -1)
          .foreach(out.write(buffer, 0, _))
        TastyFile(lib, entry.getName, out.toByteArray)
      }
      Right(tastyFiles.toList)
    finally urlIs.close()
  }

def printType(tpe: Type): String = tpe match
  case PackageRef(pck)     => pck.toString
  case TypeRef(base, name) => printType(base) + "." + name.toString
  case ThisType(base)      => printType(base) + ".this"
  case TermRef(base, name) => printType(base) + "." + name.toString
  case AppliedType(base, args) =>
    printType(base) + args.map(_ => "_").mkString("[", ", ", "]")
  case NoPrefix              => "<root>"
  case AnnotatedType(tpe, _) => printType(tpe)
  case AndType(tpe1, tpe2)   => printType(tpe1) + " & " + printType(tpe2)
  case OrType(tpe1, tpe2)    => printType(tpe1) + " | " + printType(tpe2)
  case _                     => tpe.toString

def isInteresting(tpe: Type): Boolean = tpe match
  case _: PackageRef => false
  case _             => true

case class Result(index: Int, infos: Seq[TreeInfo]):
  def ++:(infos: Seq[TreeInfo]) = copy(infos = infos ++ this.infos)

def walkTasty(
    tastyFile: TastyFile,
    t: Tree,
    index: Int,
    depth: Int,
    method: String,
    topLevel: Boolean
): Result =
  def processChildren(
      newMethod: String = method,
      index: Int = index,
      newTopLevel: Boolean = topLevel
  ) =
    t.subtrees.foldLeft(Result(index, Nil)) { (prev, tree) =>
      prev.infos ++: walkTasty(
        tastyFile,
        tree,
        prev.index,
        depth + 1,
        newMethod,
        topLevel
      )
    }

  def ignore = Result(index, Nil)

  def newMethod(name: Name) =
    if method.isEmpty then name.toString else s"$method.$name"

  def currentInfo = TreeInfo(
    tastyFile.lib,
    tastyFile.path,
    method,
    t.getClass.getSimpleName,
    depth,
    index,
    t.tpeOpt.filter(isInteresting).map(printType)
  )

  t match
    case PackageDef(pid, stats) => processChildren(newMethod(pid.name))
    case ImportSelector(imported, renamed, bound) => ignore
    case Import(expr, selectors)                  => ignore
    case Class(name, rhs, symbol) => processChildren(newMethod(name))
    case _: Template | _: Block   => processChildren()
    case ValDef(name, tpt, rhs, symbol) =>
      processChildren(newMethod(name), newTopLevel = false)
    case DefDef(name, params, tpt, rhs, symbol) =>
      processChildren(newMethod(name), newTopLevel = false)
    case _ => Seq(currentInfo) ++: processChildren(index = index + 1)

def processTastyFile(tastyFile: TastyFile): Either[String, Seq[TreeInfo]] =
  Try {
    val pickle = new TastyUnpickler(tastyFile.content).unpickle(
      new TastyUnpickler.TreeSectionUnpickler()
    )
    val ast = pickle.get.unpickle(using Contexts.empty(tastyFile.path))
    ast.flatMap(walkTasty(tastyFile, _, 0, 0, "", topLevel = true).infos)
  }.toEither.left
    .map(e =>
      s"Failure to process ${tastyFile.path} from ${tastyFile.lib}: ${e.getMessage}"
    )

@main def typesFrom(args: String*) = args match
  case Seq(org, name, version) =>
    val lib = Library(org, name, version)
    val (failed, tastyFiles) =
      val tastyFiles = loadTastyFiles(lib)
      tastyFiles.left.foreach(println)
      tastyFiles
        .fold(_ => Nil, identity)
        .map(processTastyFile)
        .partitionMap(identity)
    failed.foreach(println)
    tastyFiles.flatten.take(10).foreach(println)
    val mostPopularTypes =
      tastyFiles.flatten
        .groupBy(_.topLevelType)
        .toSeq
        .collect { case (Some(tpe), instances) => (tpe, instances.size) }
        .sortBy(-_._2)
        .take(10)
    import scala.util.chaining._
    val librarySizes =
      tastyFiles.flatten
        .groupBy(_.lib)
        .toSeq
        .map { case (lib, infos) =>
          (s"${lib.org}:${lib.name}:${lib.version}", infos.size)
        }
        .sortBy(_(1))
        .reverse

    println("\nAmount of treeinfos per library:")
    librarySizes.foreach { case (lib, size) =>
      println(s"$lib: $size")
    }
    println("\nMost popular types:")
    println(
      mostPopularTypes
        .map { case (name, count) => s"$name : $count" }
        .mkString("\n")
    )
  case other =>
    println(
      s"Expected <organization> <name> <version> but got ${args.mkString(" ")}"
    )
