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

import scala.concurrent._
import ExecutionContext.Implicits.global

import scalaz._, Scalaz._

import org.apache.hadoop.hive.conf.HiveConf

import com.twitter.scalding.{Config, Execution}
import com.twitter.scalding.typed.TypedPipe

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

import au.com.cba.omnia.omnitool.{Result, Ok, Error}

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.scalding.ConfHelper

import au.com.cba.omnia.maestro.example.thrift.Customer

/** Configuration for a customer execution example */
case class PartitionJobConfig(config: Config) {
  val maestro = MaestroConfig(
    conf      = config + (("hive.metastore.execute.setugi","true")),
    source    = "customer",
    domain    = "customer",
    tablename = "customer"
  )
  val nPartitions = config.getArgs.int("num-partitions")
  val nRecords    = config.getArgs.int("num-records")
  val table       = maestro.partitionedHiveTable[Customer, Int](
    partition = Partition.byField(Fields[Customer].Balance),
    tablename = "by_balance",
    path      = Some(maestro.hdfsRoot)
  )
}

object PartitionJob extends MaestroJob {
  
  def spawnQueryThread(conf: PartitionJobConfig) = {
    logger.info("Starting query loop.")
    val hiveConf = new HiveConf(ConfHelper.getHadoopConf(conf.config), this.getClass)
    val queriesTask = Future{
      while(true) {
        val query = s"SELECT * FROM ${conf.table.name}"
        logger.info(s"Running query: $query")
        Hive.query(query).run(hiveConf) match {
          case Ok(r)    => logger.info(s"Query returned: $r")
          case Error(e) => logger.error(s"Failed to execute query: $e")
        }
        Thread.sleep(5000)
      }
    }
  }

  def job: Execution[JobStatus] = for {
    conf  <- Execution.getConfig.map(PartitionJobConfig(_))
    _     <- Execution.value(spawnQueryThread(conf))
    pipe   = TypedPipe.from(
      Stream.from(1)
        .map(i => Customer(s"$i", None, "", "", "", i % conf.nPartitions, ""))
        .take(conf.nRecords)
    ).groupBy((x:Customer) => x.balance)
     .forceToReducers
     .map{ case (k,v) => v }
    count <- viewHive(conf.table, pipe) 
  } yield JobFinished

  def attemptsExceeded = Execution.from(JobNeverReady)   // Elided in the README
}

