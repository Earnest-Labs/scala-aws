package com.earnest.util.aws.s3

import cats.effect.{ContextShift, Effect, Resource}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.{AmazonS3Client, S3ClientOptions}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerConfiguration}
import com.earnest.util.aws.s3.config.S3ConnectionConfig

import scala.concurrent.ExecutionContext

trait Connection {
  def createS3DataSourceResource[F[_]](s3ConnConfig: S3ConnectionConfig, blockingEc: ExecutionContext)
    (implicit F: Effect[F], CS: ContextShift[F]): Resource[F, S3DataSource[F]] =
    Resource.make(createS3DataSource(s3ConnConfig, blockingEc))(s => F.delay(s.shutdown()))

  def createS3DataSource[F[_]](s3ConnConfig: S3ConnectionConfig, blockingEc: ExecutionContext)
    (implicit F: Effect[F], CS: ContextShift[F]): F[S3DataSource[F]] =
    F.delay {
      val s3ClientOptions = S3ClientOptions
        .builder()
        .setPathStyleAccess(true)
        .build()

      def createS3Client(s3Conf: S3ConnectionConfig): TransferManager = {
        val s3Client = new AmazonS3Client(new BasicAWSCredentials(s3Conf.accessKey, s3Conf.secretKey))
        s3Conf.endpoint.foreach(s3Client.setEndpoint)
        s3Client.setS3ClientOptions(s3ClientOptions)

        val manager = new TransferManager(s3Client)
        val configuration = new TransferManagerConfiguration()
        manager.setConfiguration(configuration)
        manager
      }
      S3DataSource(createS3Client(s3ConnConfig), s3ConnConfig, blockingEc, CS)
    }
}
