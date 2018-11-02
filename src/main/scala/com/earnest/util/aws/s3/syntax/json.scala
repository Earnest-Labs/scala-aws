package com.earnest.util.aws.s3.syntax

import java.io.{BufferedReader, ByteArrayInputStream, InputStream, InputStreamReader}
import java.nio.charset.Charset
import java.util.stream.Collectors

import cats.effect.Effect
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.earnest.util.aws.s3.S3DataSource
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.syntax._
import com.earnest.util.aws.s3.syntax.transfer._

import scala.language.implicitConversions

final class JsonOps[F[_]](val s3DS: S3DataSource[F]) extends AnyVal {
  /**
    * Not too happy to handle all errors here, but AWS returns a generic `AmazonS3Exception`,
    * which could be anything
    * @return None if the object is not there
    */
  def getJson[T: Decoder](key: String)(implicit F: Effect[F]): F[Option[T]] =
    F.handleError(
      s3DS.openStreamToFile(key) >>=
        (is => transformToString(is).map(s => parse(s).toOption.flatMap(_.as[T].toOption))))(_ => None)

  def upsertJson[T: Encoder](key: String, meta: T)(implicit F: Effect[F]): F[Unit] =
    F.delay(meta.asJson.noSpaces.getBytes(Charset.forName("UTF-8"))) >>=
      (jsonBytes => s3DS.upload(key, new ByteArrayInputStream(jsonBytes), jsonBytes.length).void)

  private def transformToString(is: InputStream)(implicit F: Effect[F]): F[String] = {
    s3DS.CS.evalOn(s3DS.blockingEc)(F.bracket(F.delay(new BufferedReader(new InputStreamReader(is))))(br =>
      F.delay(br.lines().collect(Collectors.joining(System.lineSeparator()))))(br =>
      F.delay(br.close())))
  }
}

trait ToJsonOps {
  implicit def toDataSourceOps[F[_]](s3DS: S3DataSource[F]): JsonOps[F] =
    new JsonOps(s3DS)
}

object json extends ToJsonOps
