package com.earnest.util.aws.sqs

import cats.data.{Validated, ValidatedNel, NonEmptyList => Nel}
import cats.effect.Effect
import cats.syntax.all._

object config {

  final case class EnvironmentKeys(
    accessKey: String,
    secretKey: String,
    queueName: String
  )

  object EnvironmentKeys {
    def default: EnvironmentKeys = EnvironmentKeys(
      accessKey = "AWS_ACCESS_KEY_ID",
      secretKey = "AWS_SECRET_ACCESS_KEY",
      queueName = "AWS_SQS_QUEUE_NAME"
    )
  }

  sealed abstract case class SQSConnectionConfig(
    accessKey: String,
    secretKey: String,
    queueName: String)

  def getFromEnvironment[F[_]](
    keys: EnvironmentKeys = EnvironmentKeys.default,
    env: Map[String, String] = sys.env)(implicit F: Effect[F]): F[SQSConnectionConfig] =
    F.delay(validate(keys, env)) >>= (validationRes =>
      validationRes.fold(errs =>
        F.raiseError[SQSConnectionConfig](new RuntimeException(errs.toList.mkString("Invalid SQS config: ", "\n", "\n"))),
        F.delay(_))
      )

  private def validate(
    envKeys: EnvironmentKeys,
    envMap: Map[String, String]): ValidatedNel[String, SQSConnectionConfig] = {

    def error(name: String): Nel[String] =
      Nel.of(s"SQS config validation error: Required environment variable '$name' not set")

    (Validated.fromOption(envMap.get(envKeys.accessKey), error(envKeys.accessKey)),
      Validated.fromOption(envMap.get(envKeys.secretKey), error(envKeys.secretKey)),
      Validated.fromOption(envMap.get(envKeys.queueName), error(envKeys.queueName))
    ) mapN
      ((accessKey: String, secretKey: String, queueName: String) =>
        new SQSConnectionConfig(accessKey, secretKey, queueName) {})
  }
}
