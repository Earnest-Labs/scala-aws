package com.earnest.util.aws.sqs

import io.circe.Decoder

/**
  * @param receiptHandle Provided ID at the point of message extraction. Should be used when deleting messages off the queue
  * @param messageId ID of the message generated at the time of delivery
  * @param md5OfPayload MD5 hash of the message payload/body
  * @param attributes Various attributes that indicate when the message was sent, by whom it was sent, etc.
  * @param payload JSON payload of the message
  */
final case class ReceivedJsonMessage[A: Decoder](
  receiptHandle: String,
  messageId: String,
  md5OfPayload: String,
  attributes: Map[String, String],
  payload: A)
