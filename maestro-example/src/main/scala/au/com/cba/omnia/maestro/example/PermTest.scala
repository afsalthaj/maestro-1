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
//
//package au.com.cba.omnia.maestro.example
//
//import java.io.StringWriter
//import collection.JavaConverters._
//
//import org.apache.hadoop.hive.conf.HiveConf
//import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
//import org.apache.hadoop.hive.metastore.api.{Database, Partition, Table}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.mapred.JobConf
//import org.apache.hadoop.fs.Path
//
//import cascading.tap.hive.HiveTableDescriptor
//import cascading.tuple.Fields
//
//import au.com.cba.omnia.permafrost.hdfs.Hdfs
//import au.com.cba.omnia.omnitool.{Result, Ok, Error}
//
//
//object PermTest {
//
//  val hdfsRoot = new Path("/tmp/maestro")
//  val conf     = new Configuration
//  val client   = getClient
//  val now      = (System.currentTimeMillis / 1000).toInt
//
//  def main(args: Array[String]) = args.toList match {
//    case db :: table :: partition :: Nil => permTest(db, table, partition)
//    case _ => println("Usage: permTest <db> <table> <partition>")
//  }
//  
//  def permTest(dbName: String, tableName: String, partitionName: String): Unit = {
//    val dbDir     = new Path(hdfsRoot, dbName)
//    val tableDir  = new Path(dbDir, tableName)
//    val db        = new Database(dbName, "", s"$dbDir", Map.empty.asJava)
//    val table     = new HiveTableDescriptor(
//      dbName, tableName, Array("id","x"), Array("int", "int"), Array("x"), ",",
//      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", tableDir
//    )
//    val partition = mkPartition("x", 1, table)
//
//    print(s"Creating DB directory: $dbDir... ")
//    Hdfs.mkdirs(dbDir).run(conf) match {
//      case Ok(_)    => println("success.")
//      case Error(e) => println(s"error: $e")
//    }
//
//    print(s"Creating database... ")
//    client.createDatabase(db)
//    println("success.")
// 
//    print(s"Creating table directory: $tableDir... ")
//    Hdfs.mkdirs(tableDir).run(conf) match {
//      case Ok(_)    => println("success.")
//      case Error(e) => println(s"error: $e")
//    }
//
//    print(s"Creating table... ")
//    client.createTable(table.toHiveTable)
//    println("success.")
//    
////    print(s"Creating partition directory: $dbDir... ")
////    Hdfs.mkdirs(tableDir).run(conf) match {
////      case Ok(_)    => println("success.")
////      case Error(e) => println(s"error: $e")
////    }
//
//    print(s"Creating partition... ")
//    client.add_partition(partition)
//    println("success.")
//  }
//  
//  def mkPartition(name: String, value: Int, table: HiveTableDescriptor): Partition = {
//    val sd = table.toHiveTable.getSd
//    sd.setLocation(s"${sd.getLocation}/$name=$value")
//    new Partition(List(name).asJava, table.getDatabaseName, table.getTableName, now, now, sd, Map.empty.asJava)
//  }
//
//  def getClient = RetryingMetaStoreClient.getProxy(
//    getHiveConf,
//    new HiveMetaHookLoader { override def getHook(tbl: Table) = null },
//    classOf[HiveMetaStoreClient].getName
//  )
//
//  def getHiveConf: HiveConf = {
//    conf.set("hive.metastore.execute.setugi","true")
//    val jobConf = new JobConf(conf)
//    val confStringWriter = new StringWriter
//    Configuration.dumpConfiguration(conf, confStringWriter)
//    confStringWriter.flush
//    println(confStringWriter)
//    new HiveConf(jobConf, this.getClass)
//  }
//
//}
//
