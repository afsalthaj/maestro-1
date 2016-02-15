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

package au.com.cba.omnia.maestro.core.codec

import scala.util.control.NonFatal

import scalaz._, Scalaz._, \&/._

import shapeless.{ProductTypeClass, TypeClassCompanion}

sealed trait Reason

case class ParseError(value: String, expected: String, error: These[String, Throwable]) extends Reason {
  override def toString = s"unexpected type. Value: $value, Expected: $expected"
}

case class NotEnoughInput(required: Int, expected: String) extends Reason {
  override def toString = s"not enough fields in record. Required: $required, Expected: $expected"
}

case object TooMuchInput extends Reason {
  override def toString = "too many fields in record"
}

sealed trait DecodeResult[A] {
  def fold[X](ok: A => X, error: (List[String], Int, Reason) => X): X = this match {
    case DecodeOk(value) => ok(value)
    case DecodeError(remainder, counter, reason) => error(remainder, counter, reason)
  }

  def map[B](f: A => B): DecodeResult[B] =
    flatMap(f andThen DecodeResult.ok)

  def flatMap[B](f: A => DecodeResult[B]): DecodeResult[B] =
    fold(f, DecodeResult.error)
}

case class DecodeOk[A](value: A) extends DecodeResult[A]

case class DecodeError[A](remainder: List[String], counter: Int, reason: Reason) extends DecodeResult[A] {
  override def toString = s"could not decode the data set $remainder: reason.toString"
}

object DecodeResult {
  def ok[A](a: A): DecodeResult[A] =
    DecodeOk(a)

  def error[A](remainder: List[String], counter: Int, reason: Reason): DecodeResult[A] =
    DecodeError(remainder, counter, reason)

  implicit def DecodeResultMonad: Monad[DecodeResult] = new Monad[DecodeResult] {
    def point[A](a: => A) = ok(a)
    def bind[A, B](m: DecodeResult[A])(f: A => DecodeResult[B]) = m flatMap f
  }

  implicit def DecodeResultEqual[A: Equal]: Equal[DecodeResult[A]] =
    Equal.equal({
      case (DecodeOk(a), DecodeOk(b)) => a === b
      case (a, b) => a == b
    })

}
