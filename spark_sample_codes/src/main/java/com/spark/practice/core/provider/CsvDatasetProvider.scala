
package com.spark.practice.core.provider

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe._

class CsvDatasetProvider[T](source: FileSource, sparkSession: SparkSession, schema: StructType, delimiter: String = CsvDatasetProvider.delimiter,
                            header: Boolean = true)(implicit tag:TypeTag[T])
  extends AbstractFileDatasetProvider[T](source, sparkSession) {

  require(schema != null)

  /**
    * @inheritdoc
    */
  override def prepareReader(): DataFrameReader = {
    sparkSession.read
  }

  private def getDelimiter(): String = delimiter

  /**
    * @inheritdoc
    */
  override protected def read(dataFrameReader: DataFrameReader, source: FileSource): Dataset[Row] = {
     dataFrameReader.schema(schema).option("delimiter", delimiter).option("header", header).csv(source.source():_*)
  }
}
object CsvDatasetProvider {
  val delimiter = "|"
}
