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

import au.com.cba.omnia.parlour.SqoopSyntax.ParlourExportDsl

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.example.thrift.Customer
 
/** Sample configuration for exporting data via sqoop. */
case class CustomerExportConfig(config: Config) {
  val envValue = EtlEnvValues.getEnvVal("LOCAL_TEST")

  val maestro   = MaestroConfig(
    conf      = config,
    source    = "customer",
    domain    = "customer",
    tablename = "customer",
    envVal    = envValue
  )
  val upload    = maestro.upload()
  val load      = maestro.load[Customer](none = "null")
  val export     = maestro.sqoopExport[ParlourExportDsl](
    dbTablename  = "customer_export"
  )
}

/** Sample job. Load customer data and export it via sqoop. */
object CustomerSqoopExportJob extends MaestroJob {
  def job: Execution[JobStatus] = {
    for {
      // Load configuration
      conf             <- Execution.getConfig.map(CustomerExportConfig(_))
      // Upload local text files to HDFS
      uploadInfo       <- upload(conf.upload)
      // Fail if there was a problem with the upload
      sources          <- uploadInfo.withSources
      // Load text files from HDFS and convert to appropriate Thrift format
      (pipe, loadInfo) <- load[Customer](conf.load, uploadInfo.files)
      // Fail if there was a problem with the load
      loadSuccess      <- loadInfo.withSuccess
      // Write out the Customers to SQL target 
      count            <- sqoopExport(conf.export, pipe)
    } yield JobFinished
  }

  def attemptsExceeded = Execution.from(JobNeverReady)
}
