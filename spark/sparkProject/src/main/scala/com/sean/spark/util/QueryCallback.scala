package com.sean.spark.util

import java.sql.ResultSet

trait QueryCallback {
  def process(rs: ResultSet)
}
