package duff

import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.Charset
import scala.util.matching.Regex
import java.nio.charset.StandardCharsets
import java.io.BufferedReader
import java.io.File
import java.nio.file.Paths
import java.nio.channels.Channels
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.FileVisitOption
import java.util.regex.Pattern
import cats.implicits._
import com.monovore.decline._

sealed trait SearchLocation
case class Rec(directory: Path) extends SearchLocation
case class Single(path: Path) extends SearchLocation
case object StdIn extends SearchLocation

case class Args(
    regex: String,
    searchLocation: SearchLocation,
    ignoreCase: Boolean,
    onlyMatch: Boolean
)

/** TODO:
  * - Use memory mapped files to maybe get a speedup
  * - gitignore support
  * - transtyping (get json output from unstructured file, maybe also parquet :) )
  * - support sql queries
  * 
  * - find files that match several bool criteria
  * - 
  */
object Main {

  def run(args: Args): Unit = {

    val Args(regexp, location, ignoreCase, onlyMatch) = args

    def searchBuffer(reader: BufferedReader, filePathPrefix: String) = {
      val re = new Regex(s"(?i)$regexp")

      val grouping = 100
      val linesStream = reader.lines().iterator().asScala.grouped(grouping)

      val results = linesStream.zipWithIndex.map {
        case (linesGroup, groupIndex) => {
          linesGroup.zipWithIndex.map { case (line, lineIndex) =>
            val lineNumber = groupIndex * grouping + lineIndex + 1
            re.findFirstMatchIn(line).foreach { currentMatch =>
              if (currentMatch.groupCount >= 1) {
                println(
                  s"$filePathPrefix${lineNumber.toString}: ${currentMatch.group(1)}"
                )
              } else {
                val line =
                  currentMatch.before.toString + Console.RED + currentMatch.matched.toString + Console.RESET + currentMatch.after
                    .toString()
                println(s"$filePathPrefix${lineNumber.toString}: $line")
              }
            }
          }

        }
      }.toList
    }
    def searchOneFile(filePath: Path, displayFileName: Boolean) = {
      val fc = FileChannel.open(filePath)

      try {

        val reader = new BufferedReader(
          Channels.newReader(fc, StandardCharsets.UTF_8)
        )

        searchBuffer(
          reader,
          if (displayFileName) s"${filePath.toString()} - " else ""
        )

      } finally {
        if (fc != null) fc.close()
      }
    }

    def searchStdIn() = {
      searchBuffer(Console.in, "")
    }

    location match {
      case Rec(directory) =>
        val searches = Files
          .walk(directory)
          .iterator()
          .asScala
          .filter(_.toFile.isFile())
          .map { file =>
            Future { searchOneFile(file, true) }
          }
        searches.foreach(_ => ())
      case Single(path) => searchOneFile(path, false)
      case StdIn        => searchStdIn()
    }

  }

  def main(mainArgs: Array[String]): Unit = {

    val recursive = Opts.flag("recursive", "", "r").orFalse
    val ignoreCase = Opts.flag("ignore-case", "", "i").orFalse

    val args = (
      Opts.argument[String]("regexp"),
      recursive,
      ignoreCase,
      Opts.argument[String]("file").orNone,
      Opts.flag("only-match", "didier", "o").orFalse
    ).tupled
      .validate("a path must be provided with --recursive") {
        case (regex, recursive, ignoreCase, path, onlyMatch) =>
          !recursive || path.isDefined
      }
      .map { case (regex, recursive, ignoreCase, path, onlyMatch) =>
        val location: SearchLocation = if (recursive) {
          path
            .map(k => Rec(Paths.get(k)))
            .getOrElse(
              sys.error("PANIC: Path is not defined, validation error")
            )
        } else if (path.isDefined) {
          path
            .map(k => Single(Paths.get(k)))
            .getOrElse(
              sys.error("PANIC: Path is not defined, validation error")
            )

        } else {
          StdIn
        }

        Args(regex, location, ignoreCase, onlyMatch)
      }

    val command = Command("", "")(args)

    command.parse(mainArgs) match {
      case Left(help) =>
        System.err.println(help)
        sys.exit(1)
      case Right(args) =>
        run(args)
    }

    ()

  }
}

//   def lol() = {
//     val count = 10485760

//     var memoryMappedFile: RandomAccessFile = null
//     try {
//       memoryMappedFile = new RandomAccessFile(path, "rw")
//       var byteBuffer =
//         memoryMappedFile.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, count)

//       val a = StandardCharsets.UTF_8.newDecoder().decode(byteBuffer);

//       // todo how to know if we have groups here
//       val re = regex.r

//       re.findAllIn(a).matchData.to(LazyList).foreach { currentMatch =>
//         if (currentMatch.groupCount > 1) {
//           println(currentMatch.group(1))
//         } else {
//           println(currentMatch.matched)
//         }
//       }

//     } finally {
//       memoryMappedFile.close()
//     }

//   }
