package com.spark.practice.model

import org.apache.spark.storage.StorageLevel

case class ExecuteArgs(mode: String = "",
                          help: Boolean = false,
                          version: Boolean = false,
                          inputLocation: String = "",
                          outputLocation: String = "",
                          processingDatadate: String = "",
                          persistModeVal: String = "",
                          appNameval: String = "",
                          prodiverId:String="",
                          debug: Boolean = false,
                          dryrun: Boolean = false,
                          var persistMode: StorageLevel = StorageLevel.NONE)
object ExecuteArgs {
  
}

