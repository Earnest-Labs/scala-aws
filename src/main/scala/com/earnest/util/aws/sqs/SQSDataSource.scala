package com.earnest.util.aws.sqs

import cats.effect.ContextShift
import com.amazonaws.services.sqs.AmazonSQS

import scala.concurrent.ExecutionContext

final case class SQSDataSource[F[_]](
  sqs: AmazonSQS,
  blockingEc: ExecutionContext,
  cs: ContextShift[F]) {

  def shutdown(): Unit = sqs.shutdown()
  def eval[A](fa: F[A]): F[A] = cs.evalOn(blockingEc)(fa)
}
