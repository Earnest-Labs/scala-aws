package com.earnest.util.aws.sqs.syntax

import cats.effect.Effect
import com.amazonaws.services.sqs.model.{SendMessageBatchRequestEntry, SendMessageBatchResult, SendMessageResult}
import com.earnest.util.aws.sqs.SQSDataSource
import com.earnest.util.aws.jsonPrinter
import io.circe.Encoder
import io.circe.syntax._

import scala.collection.JavaConverters._
import scala.language.implicitConversions

final class DeliveryOps[F[_]](val sds: SQSDataSource[F]) extends AnyVal {
  def sendMessage(queueUrl: String)(message: String)(implicit F: Effect[F]): F[SendMessageResult] =
    sds.eval(F.delay(sds.sqs.sendMessage(queueUrl, message)))

  def sendJsonMessage[E: Encoder](queueUrl: String)(message: E)(implicit F: Effect[F]): F[SendMessageResult] =
    sds.eval(F.delay(sds.sqs.sendMessage(queueUrl, message.asJson.pretty(jsonPrinter))))

  /**
    * Each message ID in a given batch should be unique
    */
  def sendJsonMessages[E: Encoder](
    queueUrl: String)(
    messageIdGenerator: () => String, messages: List[E])(implicit F: Effect[F]): F[SendMessageBatchResult] =
    sds.eval(F.delay(sds.sqs.sendMessageBatch(queueUrl,
      messages.map(message => new SendMessageBatchRequestEntry(messageIdGenerator(), message.asJson.pretty(jsonPrinter))).asJava)))
}

trait ToDeliveryOps {
  implicit def toDeliveryOps[F[_]](sqs: SQSDataSource[F]): DeliveryOps[F] =
    new DeliveryOps(sqs)
}

object delivery extends ToDeliveryOps
