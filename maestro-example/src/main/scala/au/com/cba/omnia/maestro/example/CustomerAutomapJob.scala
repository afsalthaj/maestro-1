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

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.{Customer, Account}
import au.com.cba.omnia.maestro.macros.automap

/** Configuration for a customer execution example */
case class CustomerAutomapConfig(config: Config) {
  val testPath = config.getArgs.optional("testPath")
  val envValue = EtlEnvValues.getEnvVal("LOCAL_TEST", testPath)

  val maestro   = MaestroConfig(
    conf      = config,
    source    = "customer",
    domain    = "customer",
    tablename = "customer",
    envVal    = envValue
  )
  val upload    = maestro.upload()
  val load      = maestro.load[Customer](none = "null")
  val acctTable = maestro.partitionedHiveTable[Account, (String, String, String)](
    partition   = Partition.byDate(Fields[Account].EffectiveDate),
    tablename   = "account"
  )
}

/** Customer file load job with an execution for the main program */
object CustomerAutomapJob extends MaestroJob {
  def job: Execution[JobStatus] = {
    @automap def customerToAccount (x: Customer): Account = {
      id           := x.acct
      customer     := x.id
      balance      := x.balance / 100
      balanceCents := x.balance % 100
    }

    for {
      // Load configuration
      conf             <- Execution.getConfig.map(CustomerAutomapConfig(_))
      // Upload local text files to HDFS
      uploadInfo       <- upload(conf.upload)
      // Fail Execution if there was a problem with the upload
      sources          <- uploadInfo.withSources
      // Load text files from HDFS and convert to appropriate Thrift format
      (pipe, loadInfo) <- load[Customer](conf.load, uploadInfo.files)
      // Define a map from the loaded Customers to Accounts
      acctPipe          = pipe.map(customerToAccount)
      // Fail if there was a problem with the load
      loadSuccess      <- loadInfo.withSuccess
      // Write out the results of the map to a Hive table
      count            <- viewHive(conf.acctTable, acctPipe)
      // Check that the number of rows written to Hive matches the number of rows loaded
      if count == loadSuccess.actual
    } yield JobFinished
  }

  def attemptsExceeded = Execution.from(JobNeverReady)   // Elided in the README
}
