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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

import au.com.cba.omnia.maestro.api._, Maestro._

import au.com.cba.omnia.maestro.example.thrift.Customer

/** Configuration for a customer execution example */
case class CustomerHiveConfig(config: Config) {
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
  val dateTable = maestro.partitionedHiveTable[Customer, (String, String, String)](
    partition   = Partition.byDate(Fields[Customer].EffectiveDate),
    tablename   = "by_date"
  )
  val catTable  = maestro.partitionedHiveTable[Customer, String](
    partition   = Partition.byField(Fields[Customer].Cat),
    tablename   = "by_cat"
  )
  val queries = List(
    s"""INSERT OVERWRITE TABLE ${catTable.name} PARTITION (partition_cat) SELECT id, name, acct,
       |cat, sub_cat, -10, effective_date, cat AS partition_cat FROM ${dateTable.name}""".stripMargin,
    s"SELECT COUNT(*) FROM ${catTable.name}"
  )
}

/** Customer execution example */
object CustomerHiveJob extends MaestroJob {
  /** Create an example customer execution */
  def job: Execution[JobStatus] = for {
      // Load configuration
    conf           <- Execution.getConfig.map(CustomerHiveConfig(_))
    // Upload local text files to HDFS
    uploadInfo     <- upload(conf.upload)
    // Fail execution if there was a problem with the upload
    sources        <- uploadInfo.withSources
    // Load text files from HDFS and convert to appropriate Thrift format
    (pipe, ldInfo) <- load[Customer](conf.load, uploadInfo.files)
    // Fail if there was a problem with the load
    loadSuccess    <- ldInfo.withSuccess
    // Write out the Customers to a Hive table partitioned by date and another Hive table partitioned by category
    (count1, _)    <- viewHive(conf.dateTable, pipe) zip viewHive(conf.catTable, pipe)
    // Verify that we wrote out the same amount of data as we received.
    _              <- Execution.guard(count1 == loadSuccess.actual, "Wrote out different number of reads than received.")
    // Perform some queries on the tables
    _              <- Execution.fromHive(
                        Hive.queries(conf.queries),
                        c => {
                          c.setVar(DYNAMICPARTITIONING, "true")
                          c.setVar(DYNAMICPARTITIONINGMODE, "nonstrict")
                          c.setVar(HIVEMERGEMAPFILES, "true")
                        }
                      )
  } yield JobFinished

  def attemptsExceeded = Execution.from(JobNeverReady)   // Elided in the README
}
