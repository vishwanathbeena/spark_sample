
package com.spark.practice.core.provider

import java.time.LocalDateTime

/**
  * This trait is designed to provide date and time by request.
  * */
trait DateTimeProvider {
  /**
    * @return current date time in UTC time zone
    * */
  def getCurrentUTCDateTime: LocalDateTime
}
