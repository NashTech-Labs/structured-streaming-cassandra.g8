package com.knoldus.helper

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, Row}

class CassandraSink(sparkConf: SparkConf) extends ForeachWriter[Row] {
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    print("\nOpen connection . . .\n")
    true
  }

  def process(row: Row): Unit = {
    val id = row.getInt(0)
    val name = row.getString(1)
    val designation = row.getString(2)
    val department = row.getList(3)
    val salary = row.getLong(4)
    val reportingMgr = row.getString(5)
    print(s"\nProcess new record: $row")

    val statement = QueryBuilder.insertInto("testing", "employee")
      .value("id", id)
      .value("name", name)
      .value("designation", designation)
      .value("department", department)
      .value("salary", salary)
      .value("reportingManager", reportingMgr)

        CassandraConnector(sparkConf).withSessionDo { session =>
          session.execute(statement)

    }
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
    print("\nClose connection . . .\n")
  }
}