
package com.spark.practice.core.provider

import org.apache.spark.sql.types.StructType

trait Source[T] {

  def source(): T
}

class FileSource(files: Seq[String], basePath: Option[String] = None,schema: Option[StructType] = None) extends Source[Seq[String]] {

  override def source(): Seq[String] = files

  def basePath(): Option[String] = basePath
  
  def schema(): Option[StructType] = schema
}


