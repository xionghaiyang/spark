package com.sean.spark.util

import java.sql.{Connection, PreparedStatement, ResultSet}

class MysqlProxy extends Serializable {

  private var rs: ResultSet = _
  private var pstm: PreparedStatement = _

  //执行修改语句
  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var count: Int = 0
    try {
      pstm = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstm.setObject(i + 1, params(i))
        }
        count = pstm.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    count
  }

  //执行查询语句
  def executeQuery(conn: Connection, sql: String, params: Array[Any], queryCallback: QueryCallback): Unit = {
    rs = null
    try {
      pstm = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstm.setObject(i + 1, params(i))
        }
      }
      rs = pstm.executeQuery()
      queryCallback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //关闭的方法
  def shutdown(conn: Connection): Unit = {
    DataSourceUtil.closeResource(rs, pstm, conn)
  }

}
