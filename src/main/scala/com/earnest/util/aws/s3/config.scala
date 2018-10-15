package com.earnest.util.aws.s3

import java.net.URL
import cats.data.{Validated, ValidatedNel, NonEmptyList => Nel}
import cats.syntax.all._
import cats.effect.Effect
import scala.util.Try

object config {
  final case class EnvironmentKeys(
    accessKey: String,
    secretKey: String,
    bucket: String,
    endpoint: String
  )
  object EnvironmentKeys {
    def default: EnvironmentKeys = EnvironmentKeys(
      accessKey = "AWS_ACCESS_KEY_ID",
      secretKey = "AWS_SECRET_ACCESS_KEY",
      bucket = "AWS_S3_BUCKET_NAME",
      endpoint = "AWS_S3_ENDPOINT"
    )
  }

  sealed abstract case class S3ConnectionConfig(
    accessKey: String,
    secretKey: String,
    bucket: String,
    endpoint: Option[String])

  def getFromEnvironment[F[_]](
    keys: EnvironmentKeys = EnvironmentKeys.default,
    env: Map[String, String] = sys.env)(implicit F: Effect[F]): F[S3ConnectionConfig] =
    F.delay(validate(keys, env)) >>= (validationRes =>
      validationRes.fold(errs =>
        F.raiseError[S3ConnectionConfig](new RuntimeException(errs.toList.mkString("Invalid S3 config: ", "\n", "\n"))),
        F.delay(_))
      )

  private def validate(
    envKeys: EnvironmentKeys,
    envMap: Map[String, String]): ValidatedNel[String, S3ConnectionConfig] = {

    def error(name: String): Nel[String] = Nel.of(s"S3 config validation error: Required environment variable '$name' not set")
    def isValidUrl(url: String): Boolean = Try(new URL(url)).isSuccess

    (Validated.fromOption(envMap.get(envKeys.accessKey), error(envKeys.accessKey)),
      Validated.fromOption(envMap.get(envKeys.secretKey), error(envKeys.secretKey)),
      Validated.fromOption(envMap.get(envKeys.bucket), error(envKeys.bucket)),
      Validated.valid(envMap.get(envKeys.endpoint).filter(isValidUrl))) mapN
      ((accessKey: String, secretKey: String, bucket: String, endpoint: Option[String]) =>
        new S3ConnectionConfig(accessKey, secretKey, bucket, endpoint){})
  }
}
