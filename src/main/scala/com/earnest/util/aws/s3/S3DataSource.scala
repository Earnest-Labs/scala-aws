package com.earnest.util.aws.s3

import com.amazonaws.services.s3.transfer.TransferManager
import com.earnest.util.aws.s3.config.S3ConnectionConfig

final case class S3DataSource(tfm: TransferManager, s3Conf: S3ConnectionConfig) {
  def shutdown(): Unit = tfm.shutdownNow()
}

