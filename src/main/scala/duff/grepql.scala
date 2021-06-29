package duff
package grepql

import com.monovore.decline._

import cats._
import cats.implicits._

import scala.collection.immutable.ArraySeq

object Main {
  case class Args(query: String)
  def run(args: Args): Unit = {}

  def main(mainArgs: Array[String]): Unit = {
    val args = Opts.argument[String]("query").map(query => Args(query))

    val command = Command("", "")(args)

    command.parse(ArraySeq.unsafeWrapArray(mainArgs)) match {
      case Left(help)  =>
        System.err.println(help)
        sys.exit(1)
      case Right(args) => run(args)
    }
  }

}
