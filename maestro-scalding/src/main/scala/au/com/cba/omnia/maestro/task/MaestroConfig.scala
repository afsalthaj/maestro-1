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

import scala.util.matching.Regex

import org.joda.time.DateTime

import scalaz.Monoid

import com.twitter.scrooge.ThriftStruct

import com.twitter.scalding.{Config, TupleSetter}

import au.com.cba.omnia.parlour.{ParlourExportOptions, ParlourImportOptions}

import au.com.cba.omnia.maestro.core.codec.{Decode, Tag}
import au.com.cba.omnia.maestro.core.clean.Clean
import au.com.cba.omnia.maestro.core.filter.RowFilter
import au.com.cba.omnia.maestro.core.partition.Partition
import au.com.cba.omnia.maestro.core.split.Splitter
import au.com.cba.omnia.maestro.core.time.TimeSource
import au.com.cba.omnia.maestro.core.upload.ControlPattern
import au.com.cba.omnia.maestro.core.validate.Validator
import au.com.cba.omnia.maestro.hive.HiveTable

trait EnvVal {
  val hdfRoot: String
  val localRoot: String
  val localArchiveDir: String
  val dbRawPrefix: String
  val dbStagingPrefix: String
  val dbDerivedPrefix: String
}

/**
  * Core configuration settings for loads, and factory functions for
  * task specific configurations.
  */
case class MaestroConfig(
  conf: Config,
  source: String,
  domain: String,
  tablename: String,
  loadTime: DateTime,
  etlEnvVal: EnvVal
) { self =>
  val args = conf.getArgs

  val hdfsRoot        = etlEnvVal.hdfRoot
  /** The standard directory structure: `\$source/\$domain/\$tablename` */
  val dirStructure    = s"${source}/${domain}/${tablename}"
  val hdfsLandingPath = s"$hdfsRoot/source/$dirStructure"
  val hdfsArchivePath = s"$hdfsRoot/archive/$dirStructure"

  /** Standard command line arguments, not required unless used */
  lazy val localIngestDir  = etlEnvVal.localRoot
  lazy val localArchiveDir = etlEnvVal.localArchiveDir
  lazy val dbRawPrefix     = etlEnvVal.dbRawPrefix
  lazy val dbStagingPrefix = etlEnvVal.dbStagingPrefix
  lazy val dbRaw           = s"${dbRawPrefix}_${source}_${domain}"
  lazy val dbStaging       = s"${dbStagingPrefix}_${source}_${domain}"
  lazy val connString      = args("jdbc")
  lazy val username        = args("db-user")
  lazy val password        = Option(System.getenv("DBPASSWORD")).getOrElse("")

  /**
    * Produce an upload config for this job using standard paths.
    *
    * The default paths are as follows:
    *  - localIngestPath  = `\$localIngestDir/dataFeed/\$source/\$domain`
    *  - hdfsLandingPath  = `\$hdfsRoot/source/\$dirStructure`
    *  - localArchivePath = `\$localArchiveDir/\$dirStructure`
    *  - hdfsArchivePath  = `\$hdfsRoot/archive/\$dirStructure`
    */
  def upload(
    localIngestPath: String  = s"$localIngestDir/dataFeed/$source/$domain",
    hdfsLandingPath: String  = hdfsLandingPath,
    localArchivePath: String = s"$localArchiveDir/$dirStructure",
    hdfsArchivePath: String  = hdfsArchivePath,
    tablename: String        = self.tablename,
    filePattern: String      = "{table}?{yyyyMMdd}*",
    controlPattern: Regex    = ControlPattern.default
  ): UploadConfig = UploadConfig(
    localIngestPath, hdfsLandingPath, localArchivePath, hdfsArchivePath,
    tablename, filePattern, controlPattern
  )

  /**
    * Produce a sqoop import config for this job using standard paths and database options.
    *
    * The standard database options are:
    *  - connectionString and username come from command line arguments
    *  - password comes from an environment variable
    *  - dbTablename is `\$tablename`
    *
    * The default paths are as follows:
    *  - hdfsLandingPath  = `\$hdfsRoot/source/\$dirStructure`
    *  - hdfsArchivePath  = `\$hdfsRoot/archive/\$dirStructure`
    *  - timePath         = `<year>/<month>/<day>`
    */
  def sqoopImport[T <: ParlourImportOptions[T] : Monoid](
    hdfsLandingPath: String        = hdfsLandingPath,
    hdfsArchivePath: String        = hdfsArchivePath,
    timePath: String               = loadTime.toString("yyyy/MM/dd"),
    connectionString: String       = connString,
    username: String               = username,
    password: String               = password,
    dbTablename: String            = self.tablename,
    outputFieldsTerminatedBy: Char = '|',
    nullString: String             = "",
    whereCondition: Option[String] = None,
    initialOptions: Option[T]      = None
  ): SqoopImportConfig[T] = {
    val options =  SqoopImportConfig.options[T](
      connectionString, username, password, dbTablename, outputFieldsTerminatedBy,
      nullString, whereCondition, initialOptions
    )
    SqoopImportConfig(hdfsLandingPath, hdfsArchivePath, timePath, options)
  }

  /**
    * Produce a sqoop import config for this job using standard paths and database options.
    *
    * Use it when you want to use SQL Select query to fetch data. If you do not provide `splitBy`,
    * then `numberOfMappers` is set to 1.
    *
    * The standard database options are:
    *  - connectionString and username come from command line arguments
    *  - password comes from an environment variable
    *  - dbTablename is `\$tablename`
    *
    * The default paths are as follows:
    *  - hdfsLandingPath  = `\$hdfsRoot/source/\$dirStructure`
    *  - hdfsArchivePath  = `\$hdfsRoot/archive/\$dirStructure`
    *  - timePath         = `<year>/<month>/<day>`
    */
  def sqoopImportWithQuery[T <: ParlourImportOptions[T] : Monoid](
    query: String,
    splitBy: Option[String],
    hdfsLandingPath: String        = hdfsLandingPath,
    hdfsArchivePath: String        = hdfsArchivePath,
    timePath: String               = loadTime.toString("yyyy/MM/dd"),
    connectionString: String       = connString,
    username: String               = username,
    password: String               = password,
    outputFieldsTerminatedBy: Char = '|',
    nullString: String             = "",
    initialOptions: Option[T]      = None
  ): SqoopImportConfig[T] = {
    val options = SqoopImportConfig.optionsWithQuery[T](
      connectionString, username, password, query, splitBy, outputFieldsTerminatedBy,
      nullString, initialOptions
    )
    SqoopImportConfig(hdfsLandingPath, hdfsArchivePath, timePath, options)
  }

  /**
    * Produces a sqoop export config for this job using standard database options.
    *
    * The standard database options are:
    *  - connectionString and username come from command line arguments
    *  - password comes from an environment variable
    *  - dbTablename is `\$tablename`
    */
  def sqoopExport[T <: ParlourExportOptions[T] : Monoid](
    deleteFromTable: Boolean      = false,
    connectionString: String      = connString,
    username: String              = username,
    password: String              = password,
    dbTablename: String           = self.tablename,
    inputFieldsTerminatedBy: Char = '|',
    inputNullString: String       = "",
    initialOptions: Option[T]     = None
  ): SqoopExportConfig[T] = {
    val options = SqoopEx.createSqoopExportOptions[T](
      connectionString, username, password, dbTablename, inputFieldsTerminatedBy,
      inputNullString, initialOptions
    )
    SqoopExportConfig(options, deleteFromTable)
  }

  /** Produces a load config for this job. */
  def load[A <: ThriftStruct : Decode : Tag : Manifest](
    errors: String          = s"""$hdfsRoot/errors/$dirStructure/${loadTime.toString("yyyy-MM-dd-hh-mm-ss")}""",
    splitter: Splitter      = Splitter.delimited("|"),
    timeSource: TimeSource  = TimeSource.fromDirStructure,
    loadWithKey: Boolean    = false,
    filter: RowFilter       = RowFilter.keep,
    clean: Clean            = Clean.default,
    none: String            = "",
    validator: Validator[A] = Validator.pass[A],
    errorThreshold: Double  = 0.05
  ): LoadConfig[A] = LoadConfig(
    errors, splitter, timeSource, loadWithKey, filter, clean, none,
    validator, errorThreshold
  )

  /**
    * An unpartitioned hive table with default database options.
    *
    * Uses `\$source_\$domain` as the default database name, and by default has
    * no database path.
    */
  def hiveTable[A <: ThriftStruct : Manifest](
    tablename: String    = self.tablename,
    database: String     = dbRaw,
    path: Option[String] = None
  ): HiveTable[A, A] =
    HiveTable(database, tablename, path)

  /**
    * A partitioned hive table with default database options.
    *
    * Uses `\$source_\$domain` as the default database name, and by default has
    * no database path.
    */
  def partitionedHiveTable[A <: ThriftStruct : Manifest, B : Manifest : TupleSetter](
    partition: Partition[A, B], // no default for partition
    tablename: String    = self.tablename,
    database: String     = dbRaw,
    path: Option[String] = None
  ): HiveTable[A, (B, A)] =
    HiveTable(database, tablename, partition, path)
}

object MaestroConfig {
  def apply(conf: Config, source: String, domain: String, tablename: String, envVal: EnvVal): MaestroConfig = {
    MaestroConfig(conf, source, domain, tablename, DateTime.now, envVal)
  }
}
