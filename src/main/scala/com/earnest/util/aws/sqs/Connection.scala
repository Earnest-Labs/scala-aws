package com.earnest.util.aws.sqs

import cats.effect.{ContextShift, Effect, Resource}
import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClient, AmazonSQSClientBuilder}
import com.earnest.util.aws.sqs.config.SQSConnectionConfig

import scala.concurrent.ExecutionContext

trait Connection {
  def createS3DataSourceResource[F[_]](
    sqsConnConfig: SQSConnectionConfig, blockingEc: ExecutionContext)(
    implicit F: Effect[F], CS: ContextShift[F]): Resource[F, SQSDataSource[F]] = {
    val awsCreds = new AWSCredentials {
      override def getAWSAccessKeyId: String = sqsConnConfig.accessKey
      override def getAWSSecretKey: String = sqsConnConfig.secretKey
    }
    Resource.make(
      F.delay(SQSDataSource[F](new AmazonSQSClient(awsCreds).asInstanceOf[AmazonSQS], blockingEc, CS))
    )(source => F.delay(source.sqs.shutdown()))
  }

  def createSQSDataSourceResourceWithEndpoint[F[_]](
    sqsConnConfig: SQSConnectionConfig, blockingEc: ExecutionContext, endpoint: String, region: String)(
    implicit F: Effect[F], CS: ContextShift[F]): Resource[F, SQSDataSource[F]] =
      Resource.make(createSQSDataSourceWithEndpoint[F](sqsConnConfig, blockingEc, endpoint, region))(q => F.delay(q.shutdown()))

  def createSQSDataSourceWithEndpoint[F[_]](
    sqsConnConfig: SQSConnectionConfig, blockingEc: ExecutionContext, endpoint: String, region: String)(
    implicit F: Effect[F], CS: ContextShift[F]): F[SQSDataSource[F]] =
    F.delay {
      val sqs = AmazonSQSClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(sqsConnConfig.accessKey, sqsConnConfig.accessKey)))
        .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
        .build()
      SQSDataSource[F](sqs, blockingEc, CS)
    }
}
