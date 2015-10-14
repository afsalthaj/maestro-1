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

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.maestro.example.thrift.{Customer, Account}

object CustomerAutomapJobSpec
  extends ThermometerHiveSpec
  with Records { def is = s2"""

Customer Automap Job
====================

  end to end pipeline        $pipeline

"""

  def pipeline = {
    val actualReader   = ParquetThermometerRecordReader[Account]
    val expectedReader = delimitedThermometerRecordReader[Account]('|', "null", implicitly[Decode[Account]])
    val dbRawPrefix    = "dr"

    withEnvironment(path(getClass.getResource("/customer").toString)) {
      val args = Map(
        "env" -> List("LOCAL_TEST")
      )
      executesSuccessfully(CustomerAutomapJob.job, args) must_== JobFinished

      facts(
        hiveWarehouse </> s"${dbRawPrefix}_customer_customer.db" </> "account" ==>
          recordsByDirectory(actualReader, expectedReader, "expected" </> "customer" </> "account")
      )
    }
  }
}
