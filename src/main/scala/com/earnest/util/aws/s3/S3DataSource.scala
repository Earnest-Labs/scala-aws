package com.earnest.util.aws.s3

import cats.effect.ContextShift
import com.amazonaws.services.s3.transfer.TransferManager
import com.earnest.util.aws.s3.config.S3ConnectionConfig

import scala.concurrent.ExecutionContext

final case class S3DataSource[F[_]](
  tfm: TransferManager,
  s3Conf: S3ConnectionConfig,
  blockingEc: ExecutionContext,
  cs: ContextShift[F]) {
  def shutdown(): Unit = tfm.shutdownNow()
}

