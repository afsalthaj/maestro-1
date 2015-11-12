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
import com.twitter.scalding.typed.TypedPipe

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

import au.com.cba.omnia.maestro.api._, Maestro._

import au.com.cba.omnia.maestro.example.thrift.Customer

/** Configuration for a customer execution example */
case class PartitionJobConfig(config: Config) {
  val maestro = MaestroConfig(
    conf      = config,
    source    = "customer",
    domain    = "customer",
    tablename = "customer"
  )
  val nPartitions    = 100
  val nRecords       = 10000
  val customerTable  = maestro.partitionedHiveTable[Customer, Int](
    partition   = Partition.byField(Fields[Customer].Balance),
    tablename   = "by_balance",
    path        = Some("/tmp/maestro/")
  )
}

object PartitionJob extends MaestroJob {
  def job: Execution[JobStatus] = for {
    conf  <- Execution.getConfig.map(PartitionJobConfig(_))
    pipe   = TypedPipe.from(
      Stream.from(1)
        .map(i => Customer(s"$i", None, "", "", "", i % conf.nPartitions, ""))
        .take(conf.nRecords)
    ).groupBy((x:Customer) => x.balance)
     .forceToReducers
     .map{ case (k,v) => v }
    count <- viewHive(conf.customerTable, pipe) 
  } yield JobFinished

  def attemptsExceeded = Execution.from(JobNeverReady)   // Elided in the README
}
