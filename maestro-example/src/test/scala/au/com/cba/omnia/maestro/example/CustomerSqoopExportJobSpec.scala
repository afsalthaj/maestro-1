//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.example

import scala.io.Source

import scalikejdbc.{SQL, ConnectionPool, AutoSession}

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.thermometer.core.{ThermometerSpec, Thermometer}, Thermometer._

import au.com.cba.omnia.ebenezer.ParquetLogging

import au.com.cba.omnia.maestro.test.SqoopExecutionTest

object CustomerSqoopExportJobSpec
  extends ThermometerSpec
  with ParquetLogging { def is = s2"""

CustomerSqoopExportJobSpec test
===============================

  end to end pipeline $pipeline

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val mapRedHome       = s"${System.getProperty("user.home")}/.ivy2/cache"
  val resourceUrl      = getClass.getResource("/sqoop-customer/export")
  val expectedData     = Source.fromFile(s"${resourceUrl.getPath}/expected-table.txt").getLines().toList

  def pipeline = {
    CustomerExport.tableSetup(connectionString, username, password)
    SqoopExecutionTest.setupEnv()

    withEnvironment(path(getClass.getResource("/sqoop-customer").toString)) {
      val args = Map(
        "jdbc"    -> List(connectionString),
        "db-user" -> List(username),
        "env"     -> List("LOCAL_TEST")
      )

      executesOk(CustomerSqoopExportJob.job, args)
      CustomerExport.tableData(connectionString, username, password) must containTheSameElementsAs(expectedData)
    }
  }

  object CustomerExport {
    Class.forName("org.hsqldb.jdbcDriver")

    def tableSetup(
      connectionString: String,
      username: String,
      password: String,
      table: String = "customer_export"
    ): Unit = {
      ConnectionPool.singleton(connectionString, username, password)
      implicit val session = AutoSession
      SQL(s"""
        create table $table (
          id varchar(10),
          name varchar(20),
          accr varchar(20),
          cat varchar(20),
          sub_cat varchar(20),
          balance integer)
      """).execute.apply()

      ()
    }

    def tableData(
      connectionString: String,
      username: String,
      password: String,
      table: String = "customer_export"
    ): List[String] = {
      ConnectionPool.singleton(connectionString, username, password)
      implicit val session = AutoSession
      SQL(s"select * from $table").map(rs => List(rs.string("id"), rs.string("name"), rs.string("accr"),
        rs.string("cat"), rs.string("sub_cat"), rs.int("balance")) mkString "|").list.apply()
    }
  }
}
