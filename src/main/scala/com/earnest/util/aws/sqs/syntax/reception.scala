package com.earnest.util.aws.sqs.syntax

import cats.effect.Effect
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.amazonaws.services.sqs.model._
import com.earnest.util.aws.sqs.{ReceivedJsonMessage, SQSDataSource}
import io.circe.{Decoder, jawn}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

final class ReceptionOps[F[_]](val sds: SQSDataSource[F]) extends AnyVal {
  def receiveMessage(queueUrl: String, maxNumberOfMessages: Int = 10)(implicit F: Effect[F]): F[ReceiveMessageResult] =
    sds.eval(F.delay(
      sds.sqs.receiveMessage(new ReceiveMessageRequest(queueUrl).withMaxNumberOfMessages(maxNumberOfMessages))))

  def receiveJsonMessages[D: Decoder](
    queueUrl: String, maxNumberOfMessages: Int = 10)(
    implicit F: Effect[F]): F[List[ReceivedJsonMessage[D]]] = {
    def parse(body: String): F[D] =
      (for {
        json <- jawn.parse(body)
        res <- json.as[D]
      } yield res).fold(err => F.raiseError(new RuntimeException(err.getMessage, err)), F.delay(_))

    sds.eval(F.delay(sds.sqs.receiveMessage(
      new ReceiveMessageRequest(queueUrl)
        .withMaxNumberOfMessages(maxNumberOfMessages))
      .getMessages.asScala.toList) >>=
      (_.traverse(msg => parse(msg.getBody)
        .map(ReceivedJsonMessage[D](msg.getReceiptHandle, msg.getMessageId,
          msg.getMD5OfBody, msg.getAttributes.asScala.toMap, _)))))
  }

  def deleteMessage(queueUrl: String)(messageId: String)(implicit F: Effect[F]): F[Unit] =
    sds.eval(F.delay(sds.sqs.deleteMessage(queueUrl, messageId)).void)

  def batchDeleteMessages(
    queueUrl: String)(
    requests: List[DeleteMessageBatchRequestEntry])(implicit F: Effect[F]): F[DeleteMessageBatchResult] =
    sds.eval(F.delay(sds.sqs.deleteMessageBatch(queueUrl, requests.asJava)))

  def purgeQueue(queueUrl: String)(implicit F: Effect[F]): F[Unit] =
    sds.eval(F.delay(sds.sqs.purgeQueue(new PurgeQueueRequest(queueUrl))).void)

  def deleteQueue(queueUrl: String)(implicit F: Effect[F]): F[Unit] =
    sds.eval(F.delay(sds.sqs.deleteQueue(queueUrl)).void)
}

trait ToReceptionOps {
  implicit def toReceptionOps[F[_]](sqs: SQSDataSource[F]): ReceptionOps[F] =
    new ReceptionOps(sqs)
}

object reception extends ToReceptionOps

