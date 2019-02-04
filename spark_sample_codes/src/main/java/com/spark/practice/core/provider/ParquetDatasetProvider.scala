
package com.spark.practice.core.provider

import org.apache.spark.sql._

import scala.reflect.runtime.universe._

class ParquetDatasetProvider[T](source: FileSource, sparkSession: SparkSession)(implicit tag:TypeTag[T])
  extends AbstractFileDatasetProvider[T](source, sparkSession) {



  override def prepareReader(): DataFrameReader = {
    sparkSession.read
  }

  override protected def read(dataFrameReader: DataFrameReader, source: FileSource): Dataset[Row] = {
    dataFrameReader.parquet(source.source():_*)
  }
}

