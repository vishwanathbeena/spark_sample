package com.spark.practice.core.provider

import org.apache.spark.sql.Dataset

trait DatasetProvider[T] {

  def provide(): Dataset[T]
}

