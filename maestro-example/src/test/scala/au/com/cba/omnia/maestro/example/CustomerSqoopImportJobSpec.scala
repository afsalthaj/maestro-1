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

import scalikejdbc.{SQL, AutoSession, ConnectionPool}

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourImportDsl

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.{Records, SqoopExecutionTest}
import au.com.cba.omnia.maestro.example.thrift.Customer

object CustomerSqoopImportJobSpec
  extends ThermometerHiveSpec
  with Records
  with ParquetLogging { def is = s2"""

Customer Sqoop Import Job test
==============================

  end to end pipeline $pipeline

"""
  val connectionString = "jdbc:hsqldb:mem:sqoopdb"
  val username         = "sa"
  val password         = ""
  val mapRedHome       = s"${System.getProperty("user.home")}/.ivy2/cache"

  def pipeline = {
    val actualReader   = ParquetThermometerRecordReader[Customer]
    val expectedReader = delimitedThermometerRecordReader[Customer]('|', "null", implicitly[Decode[Customer]])
    val dbRawPrefix    = "dr"

    CustomerImport.tableSetup(connectionString, username, password)
    SqoopExecutionTest.setupEnv()

    val args = Map(
      "jdbc"     -> List(connectionString),
      "db-user"  -> List(username),
      "env"      -> List("LOCAL_TEST"),
      "testPath" -> List(dir)
    )

    withEnvironment(path(getClass.getResource("/sqoop-customer/import").toString)) {
      executesSuccessfully(CustomerSqoopImportJob.job, args) === JobFinished

      facts(
        hiveWarehouse </> s"${dbRawPrefix}_sales_books.db" </> "by_cat"  ==>
          recordsByDirectory(actualReader, expectedReader, "expected" </> "by-cat", { (c: Customer) =>
            c.unsetEffectiveDate
          })
      )
    }
  }

  object CustomerImport {
    Class.forName("org.hsqldb.jdbcDriver")

    val data = List(
      "C001|Fred|0001|F|M|25",
      "C002|Barney|0002|S|M|2260",
      "C003|Home|0003|S|M|-10",
      "C004|Wilma|0004|F|F|1003",
      "C005|Betty|0005|F|F|10000",
      "C006|Marge|0006|S|F|10"
    )

    def tableSetup(connectionString: String, username: String, password: String, table: String = "customer_import"): Unit = {
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

      data.map(line => line.split('|')).foreach(
        row => SQL(s"""insert into ${table}(id, name, accr, cat, sub_cat, balance)
        values ('${row(0)}', '${row(1)}', '${row(2)}', '${row(3)}', '${row(4)}', '${row(5)}')""").update().apply())
    }
  }
}
