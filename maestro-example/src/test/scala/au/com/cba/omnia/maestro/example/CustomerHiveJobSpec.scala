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

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec
import au.com.cba.omnia.thermometer.fact.PathFactoids._

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.maestro.example.thrift.Customer

object CustomerHiveJobSpec
  extends ThermometerHiveSpec
  with Records
  with ParquetLogging { def is = s2"""

Customer Hive Job
=================

  end to end pipeline        $pipeline

"""

  def pipeline = {
    val actualReader      = ParquetThermometerRecordReader[Customer]
    val expectedReader    = delimitedThermometerRecordReader[Customer]('|', "null", implicitly[Decode[Customer]])
    val dbRawprefix       = "dr"
    val customerWarehouse = hiveWarehouse </> s"${dbRawprefix}_customer_customer.db"
    val expectedDir       = "expected" </> "customer"

    withEnvironment(path(getClass.getResource("/customer").toString)) {
      val args = Map(
        "env"            -> List("LOCAL_TEST")
      )
      executesSuccessfully(CustomerHiveJob.job, args) must_== JobFinished

      facts(
        customerWarehouse </> "by_date" ==> recordsByDirectory(actualReader, expectedReader, expectedDir </> "by-date"),
        customerWarehouse </> "by_cat"  ==> recordsByDirectory(actualReader, expectedReader, expectedDir </> "by-cat")
      )
    }
  }
}
