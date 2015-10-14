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

import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.parlour.ParlourImportOptions

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.Customer

/** Configuration for `CustomerSqoopImportExecution` */
case class CustomerImportConfig(config: Config) {
  val envValue = EtlEnvValues.getEnvVal(config.getArgs("env"))
  val maestro  = MaestroConfig(
    conf      = config,
    source    = "sales",
    domain    = "books",
    tablename = "customer_import",
    envVal    = envValue
  )
  val sqoopImport  = maestro.sqoopImport(
    initialOptions = Some(ParlourImportDsl().splitBy("id"))
  )
  val load        = maestro.load[Customer](
    none          = "null"
  )
  val catTable    = maestro.partitionedHiveTable[Customer, String](
    partition     = Partition.byField(Fields[Customer].Cat),
    tablename     = "by_cat"
  )
}

/** Example customer execution, importing data via Sqoop */
object CustomerSqoopImportJob extends MaestroJob {
  /** Create an example customer sqoop import execution */
  def job: Execution[JobStatus] = for {
    // Load configuration
    conf               <- Execution.getConfig.map(CustomerImportConfig(_))
    // Import data from SQL source to HDFS
    (path, sqoopCount) <- sqoopImport(conf.sqoopImport)
    // Load data from HDFS and convert to appropriate Thrift format
    (pipe, loadInfo)   <- load[Customer](conf.load, List(path))
    // Fail if there was a problem with the load
    loadSuccess        <- loadInfo.withSuccess
    // Write out the Customers to a Hive table
    hiveCount          <- viewHive(conf.catTable, pipe)
  } yield JobFinished

  def attemptsExceeded = Execution.from(JobNeverReady)   // Elided in the README
}
