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

package au.com.cba.omnia.maestro.scalding

import scala.concurrent.Future

import scalaz.{\/, Monad}
import scalaz.\&/.{These, This, That, Both}

import com.twitter.scalding.{Args, Config, Execution}

import org.apache.hadoop.hive.conf.HiveConf

import au.com.cba.omnia.omnitool.{Result, ResultantMonad, ResultantOps, ResultantMonadOps}

import au.com.cba.omnia.permafrost.hdfs.Hdfs

import au.com.cba.omnia.ebenezer.scrooge.hive.Hive

/** Pimps an Execution instance. */
case class RichExecution[A](execution: Execution[A]) {
  def withSubConfig(modifyConfig: Config => Config): Execution[A] =
    Execution.getConfigMode.flatMap { case (config, mode) =>
      Execution.fromFuture(cec => execution.run(modifyConfig(config), mode)(cec))
    }

  /** Modify args only */
  def withSubArgs(modifyArgs: Args => Args): Execution[A] = {
    withSubConfig(config => config.setArgs(modifyArgs(config.getArgs)))
  }
}

/** Pimps the Execution object. */
case class RichExecutionObject(exec: Execution.type) extends ResultantOps[Execution] {
  implicit val monad: ResultantMonad[Execution] = ExecutionOps.ExecutionResultantMonad

  /** Alias for [[fromHdfs]] */
  def hdfs[T](action: Hdfs[T]): Execution[T] =
    fromHdfs(action)

  /** Changes from HDFS context to Execution context. */
  def fromHdfs[T](hdfs: Hdfs[T]): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail

    Execution.getConfig.flatMap(config =>
      Execution.fromFuture { _ =>
        val result = hdfs.run(ConfHelper.getHadoopConf(config)).addMessage("HDFS operation failed")
        resultToFuture(result, stacktrace)
      }
    )
  }

  /** Alias for [[fromHive]] */
  def hive[T](action: Hive[T], modifyConf: HiveConf => Unit = _ => ()): Execution[T] =
    fromHive(action, modifyConf)

  /**
    * Changes from Hive context to Execution context.
    *
    * `modifyConf` can be used to update the hive conf that will be used to run the Hive action.
    */
  def fromHive[T](hive: Hive[T], modifyConf: HiveConf => Unit = _ => ()): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail

    Execution.getConfig.flatMap { config =>
      val hiveConf = new HiveConf(ConfHelper.getHadoopConf(config), this.getClass)
      modifyConf(hiveConf)
      Execution.fromFuture[T] { _ =>
        val result = hive.run(hiveConf).addMessage("Hive Operation failed")
        resultToFuture(result, stacktrace)
      }
    }
  }

  /** Changes from an action that produces a Result to an Execution. */
  def fromResult[T](result: => Result[T]): Execution[T] = {
    /* Need to get the stack trace outside of the execution so we get a stack trace that is from the
     * main thread which includes the stack that called this function.
     */
    val stacktrace = Thread.currentThread.getStackTrace.tail
    Execution.fromFuture[T](_ => resultToFuture(result, stacktrace))
  }

  /** Alias for [[fromEither]] */
  def either[T](disjunction: => String \/ T): Execution[T] =
    fromEither(disjunction)

  /** Changes from an action that produces a scalaz Disjunction to an Execution. */
  def fromEither[T](disjunction: => String \/ T): Execution[T] =
    fromResult(disjunction.fold(Result.fail, Result.ok))

  /**
    * Helper function to convert a result to a future.
    *
    * For errors it creates a top level exception and fills it in with the provided stack trace.
    * It also prefixes the exception message with the provided prefix.
    */
  def resultToFuture[T](result: Result[T], stacktrace: Array[StackTraceElement]): Future[T] =
    result.fold(
      Future.successful,
      error => Future.failed(ResultException.fromError(error, stacktrace))
    )
}

object ExecutionOps extends ExecutionOps

trait ExecutionOps {
  /** Implicit conversion of an Execution instance to RichExecution. */
  implicit def executionToRichExecution[A](execution: Execution[A]): RichExecution[A] =
    RichExecution[A](execution)

  /** Implicit conversion of Execution Object to RichExecutionObject. */
  implicit def ExecutionToRichExecution(exec: Execution.type): RichExecutionObject =
    RichExecutionObject(exec)

  implicit val scalazExecutionMonad: Monad[Execution] = new Monad[Execution] {
    def point[A](v: => A) = Execution.from(v)
    def bind[A, B](a: Execution[A])(f: A => Execution[B]) = a.flatMap(f)
  }

  implicit def ExecutionResultantMonad: ResultantMonad[Execution] = new ResultantMonad[Execution] {
    def rPoint[A](v: => Result[A]): Execution[A] = Execution.fromResult(v)

    def rBind[A, B](ma: Execution[A])(f: Result[A] => Execution[B]): Execution[B] =
      ma.map(Result.ok(_))
        .recoverWith[Result[A]]{ case thr => Execution.from(Result.exception[A](thr)) }
        .flatMap(f)       // flatMap over an Execution[Result[A]] that is overall equivalent to ma.
  }

  /** Pimps a [[ResultantMonad]] to have access to the functions in [[ResultantMonadOps]]. */
  implicit def ToResultantMonadOps[M[_], A](v: M[A])(implicit M0: ResultantMonad[M]): ResultantMonadOps[M, A] =
    new ResultantMonadOps[M, A](v)
}

/**
  * Wraps up Results in a consistent way that also includes a stack trace of the place that produced
  * the result.
  */
case class ResultException(msg: String, stacktrace: Array[StackTraceElement], exception: Option[Throwable] = None)
    extends Exception(msg, exception.getOrElse(null)) {
  setStackTrace(stacktrace)
}

/**
  * These helper methods provide a lossy mapping from [[Result]] to an exception Execution can
  * handle and a back again.
  *
  * These helper methods map any result to a ResultException. If the result already contains a
  * ResultException that is used, otherwise a new ResultException is created with the provided stack
  * trace.
  * All three error cases from Result are mapped to a ResultException. However, when mapping from
  * ResultException to Result only the `This` error case is recovered, where the exception is a
  * ResultException.
  */
object ResultException {
  val prefix = "Result failure"

  /** Converts a Result Error to a ResultException. See object level comments for details. */
  def fromError(error: These[String, Throwable], stacktrace: Array[StackTraceElement]): ResultException =
    error match {
      case This(msg)     => ResultException(msg, stacktrace)
      case That(ex)      => liftThrowable(ex, stacktrace)
      case Both(msg, ex) => liftThrowable(ex, stacktrace, msg)
    }

  /** Converts an Exception to a ResultException. See object level comments for details. */
  def liftThrowable(
    throwable: Throwable, stacktrace: Array[StackTraceElement],
    msg: String = prefix
  ): ResultException =
    throwable match {
      case r: ResultException => r
      case ex                 => ResultException(msg, stacktrace, Some(ex))
    }
}
