package com.earnest.util.aws.sqs.syntax

import cats.effect.Effect
import com.amazonaws.services.sqs.model.{GetQueueAttributesRequest, GetQueueAttributesResult, ListQueuesResult}
import com.earnest.util.aws.sqs.SQSDataSource

import scala.language.implicitConversions

final class QueryOps[F[_]](val sds: SQSDataSource[F]) extends AnyVal {
  def getQueueUrl(queueName: String)(implicit F: Effect[F]): F[String] =
    sds.eval(F.delay(sds.sqs.getQueueUrl(queueName).getQueueUrl))

  def getQueueAttributes(request: GetQueueAttributesRequest)(implicit F: Effect[F]): F[GetQueueAttributesResult] =
    sds.eval(F.delay(sds.sqs.getQueueAttributes(request)))

  def listQueues(queuePrefix: String)(implicit F: Effect[F]): F[ListQueuesResult] =
    sds.eval(F.delay(sds.sqs.listQueues(queuePrefix)))
}

trait ToQueryOps {
  implicit def toQueryOps[F[_]](sqs: SQSDataSource[F]): QueryOps[F] =
    new QueryOps(sqs)
}

object query extends ToQueryOps
