package au.com.cba.omnia.maestro.example

import au.com.cba.omnia.maestro.task.EnvVal

case class EtlEnvValues(hdfRoot: String, localRoot: String, localArchiveDir: String, dbRawPrefix: String, dbStagingPrefix: String, dbDerivedPrefix: String) extends EnvVal

object EtlEnvValues {
  /* method to fetch variables based on environment */
  def getEnvVal(env: String, dir: Option[String]): EtlEnvValues =  (env, dir) match {
    case ("LOCAL_TEST", Some(path))  => EtlEnvValues(hdfRoot=s"${path}/user", localRoot=s"${path}/user", localArchiveDir=s"${path}/user/archive", dbRawPrefix="dr", dbStagingPrefix="ds", dbDerivedPrefix="dd")
    case ("DEV", _)                  => EtlEnvValues(hdfRoot="/dev", localRoot="/user1/dev", localArchiveDir="/user1/dev/archive", dbRawPrefix="dr", dbStagingPrefix="ds", dbDerivedPrefix="dd")
    case (_, _)                      => throw  new Exception("Environment Not Set Properly")
  }
}