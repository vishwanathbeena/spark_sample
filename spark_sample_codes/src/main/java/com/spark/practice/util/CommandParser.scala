package com.spark.practice.util


import org.apache.spark.storage.StorageLevel
import scopt.OptionParser
import com.spark.practice.model.ExecuteArgs


object CommandParser {
  
  def parseInputArgs(args: Array[String]): Option[ExecuteArgs] = {
    val executeArgs = ExecuteArgs()
    parser.parse(args, executeArgs)
  }
  
  val parser = new OptionParser[ExecuteArgs]("CharterNcsParser") {
    head(s"${BuildInfo.name} v${BuildInfo.version} is © 2018 by ${BuildInfo.organization}")
    opt[Unit]("version") abbr ("v") action ((_, c) => c.copy(version = true)) text ("Prints the Join Application version that the Join program came from.")

    cmd("execute") action ((_, c) => c.copy(mode = "execute")) children(
      opt[String]("input-location") abbr ("i") action ((x, c) => c.copy(inputLocation = x)) required() text ("Input path whether the input datasets are stored for processing."),
      opt[String]("output-location") abbr ("o") action ((x, c) => c.copy(outputLocation = x)) required() text ("Output path whether the output datasets needs to be stored."),
      opt[String]("processing-datadate") abbr ("l") action ((x, c) => c.copy(processingDatadate = x)) required() text ("processesing data date required. "),
      opt[String]("persist-mode") abbr ("p") action ((x, c) => c.copy(persistModeVal = x)) required() text ("Persist method for the data. Can be any value from org.apache.spark.storage.StorageLevel."),
      opt[String]("app-name") abbr ("a") action ((x, c) => c.copy(appNameval = x)) required() text ("application Name to be submitted to spark engine while running the job"),
      opt[String]("provider-id") abbr ("k") action ((x, c) => c.copy(prodiverId = x)) required() text ("provider-id to be submitted to spark engine while running the job"),
      opt[Unit]("debug") abbr ("d") action ((_, c) => c.copy(debug = true)) text ("Enables debug for the application to store all the intermediate data."),
      opt[Unit]("dryrun") abbr ("r") action ((_, c) => c.copy(dryrun = true)) text ("Enables dryrun to check the syntax and no actual processing is performed.")
    ) text ("execute - invokes the actual processing of CharterNcsParser")

    help("help") abbr ("?") action ((_, c) => c.copy(help = true)) text ("prints this usage text")

  }

}
