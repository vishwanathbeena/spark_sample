package com.spark.practice.core.provider

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.runtime.universe._


abstract class AbstractFileDatasetProvider[T](source: FileSource, sparkSession: SparkSession)(implicit tag:TypeTag[T]) extends DatasetProvider[T]{

  require(source != null)
  require(sparkSession != null)

  private implicit val encoder: Encoder[T] = if (tag.tpe != typeOf[Row]) ExpressionEncoder[T] else null

  protected def prepareReader(): DataFrameReader

  protected def read(dataFrameReader: DataFrameReader, source: FileSource): Dataset[Row]

  final override def provide(): Dataset[T] = {
    val reader = prepareReader()
    if (!source.basePath().isEmpty) {
      reader.option("basePath", source.basePath().get)
    }
    if (!source.schema().isEmpty) {
      reader.schema(source.schema().get)
    }
    val result = read(reader, source)
    if (tag.tpe != typeOf[Row]) {
      result.as[T]
    } else {
      result.asInstanceOf[Dataset[T]]
    }
  }

}