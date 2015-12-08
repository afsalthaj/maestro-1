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

package au.com.cba.omnia.maestro.task

import scalaz.Scalaz._

import com.twitter.scalding.{Execution, TypedPipe, TypedCsv}
import com.twitter.scalding.TDsl._

import org.specs2.matcher.Matcher

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerSource}, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoid
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import au.com.cba.omnia.ebenezer.ParquetLogging
import au.com.cba.omnia.ebenezer.scrooge.hive._
import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

import au.com.cba.omnia.omnitool.{Result, Ok, Error}

import au.com.cba.omnia.maestro.core.data.Field
import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.hive.{HiveTable, UnpartitionedHiveTable}
import au.com.cba.omnia.maestro.scalding.ExecutionOps._
import au.com.cba.omnia.maestro.core.thrift.scrooge.StringPair

private object ViewExec extends ViewExecution

object ViewExecutionSpec extends ThermometerHiveSpec with ParquetLogging { def is = s2"""
View execution properties
=========================
    can append to a hive table using parallel execution                    $sequencedHiveAppend

"""

def skippedTests = s2"""
View execution properties
=========================

  partitioned:
    can write using execution monad                                      $normal
    view executions can be composed with zip                             $zipped

    can write to a hive table using execution monad                      $normalHive
    can overwrite to a hive table using execution monad                  $normalHiveOverwrite
      * this verifies that existing data in a partition gets overwritten
      * that if multiple files in a partition are being overwritten by less files
        the additional files are cleaned up
      * that existing data in a partition without new data is untouched
    can overwrite to an already created hive table                       $createdHiveOverwrite
    can append to a hive table using execution monad                     $normalHiveAppend
    can append to a hive table using zipped execution                    $zippedHiveAppend
    view hive executions can be composed with flatMap                    $flatMappedHive
    view hive executions can be composed with zip                        $zippedHive
    can write to tables where the underlying folder has been deleted     $withoutFolder

  unpartitioned:
    can write to a hive table using execution monad                      $normalHiveUnpartitioned
    can append to a hive table using execution monad                     $normalHiveUnpartitionedAppend
    can append to a hive table using zipped execution                    $zippedHiveUnpartitionedAppend
    view hive executions can be composed with flatMap                    $flatMappedHiveUnpartitioned
    view hive executions can be composed with zip                        $zippedHiveUnpartitioned
    can write to tables where the underlying folder has been deleted     $withoutFolderUnpartitioned
"""

  // Partitioned tests

  def normal = {
    val exec = ViewExec.view(ViewConfig(byFirst, s"$dir/normal"), source)
    executesSuccessfully(exec) must_== 4
    facts(
      dir </> "normal" </> "A" </> "part-*.parquet" ==> matchesFile,
      dir </> "normal" </> "B" </> "part-*.parquet" ==> matchesFile
    )
  }

  def zipped = {
    val exec = ViewExec.view(ViewConfig(byFirst, s"$dir/zipped/by_first"), source)
      .zip(ViewExec.view(ViewConfig(bySecond, s"$dir/zipped/by_second"), source))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      dir </> "zipped" </> "by_first"  </> "A" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_first"  </> "B" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_second" </> "1" </> "part-*.parquet" ==> matchesFile,
      dir </> "zipped" </> "by_second" </> "2" </> "part-*.parquet" ==> matchesFile
    )
  }

  def normalHive = {
    val exec = for {
      a <- ViewExec.viewHive(tableByFirst("normalHive"), source)
      b <- ViewExec.viewHive(tableByFirst("normalHive2"), source, false)
    } yield (a, b)

    executesSuccessfully(exec) must_== ((4, 4))

    facts(
      hiveWarehouse </> "normalhive.db"  </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive.db"  </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive2.db" </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive2.db" </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> matchesFile
    )
  }

  // Uses withEnvironment so that we can force multiple map tasks to run and write out multiple files.
  def normalHiveOverwrite = withEnvironment(path(getClass.getResource("/view-execution").toString)) {
    val expectedA = List(StringPair("A", "10"))
    val expectedB = List(StringPair("B", "10"), StringPair("B", "11"))
    val expectedC = List(StringPair("C", "1"), StringPair("C", "2"), StringPair("C", "3"), StringPair("C", "4"))

    val s1 = TypedCsv[(String, String)]("partitioned-overwrite1").map { case (x, y) => StringPair(x, y) }
    val s2 = TypedCsv[(String, String)]("partitioned-overwrite2").map { case (x, y) => StringPair(x, y) }
    val exec = for {
      c1 <- ViewExec.viewHive(tableByFirst("normalHive"), s1, false)
      c2 <- ViewExec.viewHive(tableByFirst("normalHive"), s2, false)
    } yield (c1, c2)

    executesSuccessfully(exec) must_== ((10, 3))

    val result = Hive.query("SELECT * FROM normalhive.by_first").map(_.length)
    result must beValue(expectedA.length + expectedB.length + expectedC.length)

    val path = hiveWarehouse </> "normalhive.db" </> "by_first"
    facts(
      path </> "partition_first=A" </> "part-*.parquet" ==> records(ParquetThermometerRecordReader[StringPair], expectedA),
      path </> "partition_first=B" </> "part-*.parquet" ==> records(ParquetThermometerRecordReader[StringPair], expectedB),
      path </> "partition_first=C" </> "part-*.parquet" ==> records(ParquetThermometerRecordReader[StringPair], expectedC)
    )
  }

  def createdHiveOverwrite = {
    val init = for {
      _  <- Hive.createParquetTable[StringPair]("normalHive", "by_first", List("partition_first" -> "string"), None)
    } yield ()

    init must beValue(())

    val exec = for {
      c1 <- ViewExec.viewHive(tableByFirst("normalHive"), source)
      c2 <- ViewExec.viewHive(tableByFirst("normalHive"), source2, false)
    } yield (c1, c2)

    executesSuccessfully(exec) must_== ((4, 2))

    val result = Hive.query("SELECT * FROM normalhive.by_first").map(_.length) must beValue(4)

    facts(
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 2),
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 2)
    )
  }

  def normalHiveAppend = {
    val exec = for {
      c1 <- ViewExec.viewHive(tableByFirst("normalHive"), source)
      c2 <- ViewExec.viewHive(tableByFirst("normalHive"), source)
    } yield (c1, c2)

    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 4),
      hiveWarehouse </> "normalhive.db" </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 4)
    )
  }

  def zippedHiveAppend = {
    val exec = ViewExec.viewHive(tableByFirst("zippedHive"), source)
      .zip(ViewExec.viewHive(tableByFirst("zippedHive"), source))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 4),
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=B"  </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 4)
    )
  }

  def sequencedHiveAppend = {
    val sequenceLength = 20
    val tuplesPerViewHive = 50000
    def exec(offset: Int) = List.tabulate(sequenceLength)((i: Int) => ViewExec.viewHive(
      tableByFirst("zippedHive"),
      ThermometerSource(Seq.tabulate(tuplesPerViewHive)((j: Int) => StringPair("A", (i*tuplesPerViewHive + j + offset).toString)))
    )).sequence

    executesSuccessfully(exec(0) zip exec(sequenceLength * tuplesPerViewHive))
    facts(
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], sequenceLength * tuplesPerViewHive * 2)
    )
  }

  def flatMappedHive = {
    val exec = for {
      count1 <- ViewExec.viewHive(tableByFirst("flatMappedHive"), source)
      count2 <- ViewExec.viewHive(tableBySecond("flatMappedHive"), source)
    } yield (count1, count2)
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "flatmappedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_first"  </> "partition_first=B"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_second" </> "partition_second=1" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhive.db" </> "by_second" </> "partition_second=2" </> "part-*.parquet" ==> matchesFile
    )
  }

  def zippedHive = {
    val exec = ViewExec.viewHive(tableByFirst("zippedHive"), source)
      .zip(ViewExec.viewHive(tableBySecond("zippedHive"), source))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=A"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_first"  </> "partition_first=B"  </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_second" </> "partition_second=1" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhive.db" </> "by_second" </> "partition_second=2" </> "part-*.parquet" ==> matchesFile
    )
  }

  def withoutFolder = {
    val exec = for {
      _ <- Execution.fromHive(Hive.createParquetTable[StringPair]("normalhive",  "by_first", List("partition_first" -> "string")))
      _ <- Execution.fromHive(Hive.createParquetTable[StringPair]("normalhive2", "by_first", List("partition_first" -> "string")))
      _ <- Execution.fromHdfs(Hdfs.delete(s"$hiveWarehouse/normalhive.db".toPath,  true))
      _ <- Execution.fromHdfs(Hdfs.delete(s"$hiveWarehouse/normalhive2.db".toPath, true))
      a <- ViewExec.viewHive(tableByFirst("normalHive"), source)
      b <- ViewExec.viewHive(tableByFirst("normalHive2"), source, false)
    } yield (a, b)

    executesSuccessfully(exec) must_== ((4, 4))

    facts(
      hiveWarehouse </> "normalhive.db"  </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive.db"  </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive2.db" </> "by_first" </> "partition_first=A" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "normalhive2.db" </> "by_first" </> "partition_first=B" </> "part-*.parquet" ==> matchesFile
    )
  }

  // Unpartitioned tests

  def normalHiveUnpartitioned = {
    val exec = for {
      a <- ViewExec.viewHive(tableUnpartitioned("unpart"),  source)
      b <- ViewExec.viewHive(tableUnpartitioned("unpart2"), source, false)
    } yield (a, b)
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "unpart.db"  </> "unpart_table" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "unpart2.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile
    )
  }

  def normalHiveUnpartitionedAppend = {
    val exec = for {
      c1 <- ViewExec.viewHive(tableUnpartitioned("unpart"), source)
      c2 <- ViewExec.viewHive(tableUnpartitioned("unpart"), source)
    } yield (c1, c2)
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "unpart.db" </> "unpart_table" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 8)
    )
  }

  def zippedHiveUnpartitionedAppend = {
    val exec = ViewExec.viewHive(tableUnpartitioned("unpart"), source)
      .zip(ViewExec.viewHive(tableUnpartitioned("unpart"), source))

    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "unpart.db" </> "unpart_table" </> "part-*.parquet" ==> recordCount(ParquetThermometerRecordReader[StringPair], 8)
    )
  }

  def flatMappedHiveUnpartitioned = {
    val exec = for {
      count1 <- ViewExec.viewHive(tableUnpartitioned("flatMappedHiveUnpart1"), source)
      count2 <- ViewExec.viewHive(tableUnpartitioned("flatMappedHiveUnpart2"), source)
    } yield (count1, count2)
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "flatmappedhiveunpart1.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "flatmappedhiveunpart2.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile
    )
  }

  def zippedHiveUnpartitioned = {
    val zipData = List(
      StringPair("C", "3"),
      StringPair("C", "4"),
      StringPair("D", "3"),
      StringPair("D", "4")
    )
    val zipSource = ThermometerSource(zipData)

    val exec = ViewExec.viewHive(tableUnpartitioned("zippedHiveUnpart"), source)
      .zip(ViewExec.viewHive(tableUnpartitioned("zippedHiveUnpart2"), zipSource))
    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "zippedhiveunpart.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "zippedhiveunpart2.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile
    )
  }

  def withoutFolderUnpartitioned = {
    val exec = for {
      _ <- Execution.fromHive(Hive.createParquetTable[StringPair]("unpart",  "unpart_table", List.empty))
      _ <- Execution.fromHive(Hive.createParquetTable[StringPair]("unpart2", "unpart_table", List.empty))
      _ <- Execution.fromHdfs(Hdfs.delete(s"$hiveWarehouse/unpart.db".toPath,  true))
      _ <- Execution.fromHdfs(Hdfs.delete(s"$hiveWarehouse/unpart2.db".toPath, true))
      a <- ViewExec.viewHive(tableUnpartitioned("unpart"),  source)
      b <- ViewExec.viewHive(tableUnpartitioned("unpart2"), source, false)
    } yield (a, b)

    executesSuccessfully(exec) must_== ((4, 4))
    facts(
      hiveWarehouse </> "unpart.db"  </> "unpart_table" </> "part-*.parquet" ==> matchesFile,
      hiveWarehouse </> "unpart2.db" </> "unpart_table" </> "part-*.parquet" ==> matchesFile
    )
  }


  // Helper methods

  def tableUnpartitioned(database: String) =
    HiveTable[StringPair](database, "unpart_table", None)

  def tableByFirst(database: String) =
    HiveTable(database, "by_first", byFirst, None)

  def tableBySecond(database: String) =
    HiveTable(database, "by_second", bySecond, None)

  def byFirst  = Partition.byField(Field[StringPair, String]("first", _.first))
  def bySecond = Partition.byField(Field[StringPair, String]("second", _.second))

  def source = ThermometerSource(data)
  def data = List(
    StringPair("A", "1"),
    StringPair("A", "2"),
    StringPair("B", "1"),
    StringPair("B", "2")
  )

  def source2 = ThermometerSource(data2)
  def data2 = List(
    StringPair("B", "11"),
    StringPair("B", "22")
  )

  def matchesFile = PathFactoid((context, path) => !context.glob(path).isEmpty)
  def noMatch = PathFactoid((context, path) => context.glob(path).isEmpty)

  def beResult[A](expected: Result[A]): Matcher[Hive[A]] =
    (h: Hive[A]) => h.run(hiveConf) must_== expected

  def beValue[A](expected: A): Matcher[Hive[A]] =
    beResult(Result.ok(expected))
}
