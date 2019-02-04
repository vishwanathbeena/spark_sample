
package com.spark.practice.core.provider

import java.time.{LocalDateTime, ZoneOffset, ZonedDateTime}

/**
  * This class is an implementation of [[DateTimeProvider]]
  * */
object DateTimeProviderService extends DateTimeProvider {
  override def getCurrentUTCDateTime: LocalDateTime = ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime
}
