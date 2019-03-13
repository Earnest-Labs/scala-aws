package com.earnest.util.aws.sqs

import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.IO
import cats.syntax.flatMap._
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry
import com.earnest.util.aws.sqs.syntax.all._
import com.earnest.util.aws.sqs.{config => sqsConfig, connection => sqsConnection}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}

import scala.concurrent.ExecutionContext

object sqsEnv {
  val tp = Executors.newFixedThreadPool(2)
  val blockingEc = ExecutionContext.fromExecutor(tp)
  implicit val cs = IO.contextShift(ExecutionContext.global)

  val sqsDS =
    (sqsConfig.getFromEnvironment[IO]() >>=
      (sqsConf =>
        sqsConnection
          .createSQSDataSourceWithEndpoint[IO](sqsConf, blockingEc, "http://sqs:9324", "us-east-1"))
      ).unsafeRunSync()

  val queueUrl = sqsDS.getQueueUrl(sys.env("AWS_SQS_QUEUE_NAME")).unsafeRunSync()
}

final class SQSInteractionTest extends FreeSpec with Matchers with GeneratorDrivenPropertyChecks with BeforeAndAfterAll {
  val tp = Executors.newFixedThreadPool(2)
  val blockingEc = ExecutionContext.fromExecutor(tp)
  implicit val cs = IO.contextShift(ExecutionContext.global)

  val sqsDS =
    (sqsConfig.getFromEnvironment[IO]() >>=
      (sqsConf =>
        sqsConnection
          .createSQSDataSourceWithEndpoint[IO](sqsConf, blockingEc, "http://sqs:9324", "us-east-1"))
      ).unsafeRunSync()

  val queueUrl = sqsDS.getQueueUrl(sys.env("AWS_SQS_QUEUE_NAME")).unsafeRunSync()

  override def afterAll() = {
    sqsDS.shutdown()
    tp.shutdownNow()
    ()
  }

  s"SQS client" - {
    "should be able to retrieve and delete sent messages" in forAll(Gen.chooseNum(1, 10).suchThat( _ > 0)) { messageCount =>
      val message = SampleMessage("foo")

      val sendResult = sqsDS.sendJsonMessages(queueUrl)(() => UUID.randomUUID().toString, List.fill(messageCount)(message))
        .unsafeRunSync()
      sendResult.getSuccessful.size() shouldBe messageCount

      val list = sqsDS.receiveJsonMessages[SampleMessage](queueUrl).unsafeRunSync()

      list.size shouldBe messageCount
      list.forall(_.payload == message) shouldBe true

      val deletionResult =
        sqsDS.batchDeleteMessages(queueUrl)(list.map(msg => new DeleteMessageBatchRequestEntry(msg.messageId, msg.receiptHandle)))
          .unsafeRunSync()

      deletionResult.getSuccessful.size() shouldBe messageCount
      deletionResult.getFailed.size() shouldBe 0
    }
  }
}

final case class SampleMessage(msg: String)
object SampleMessage {
  implicit val sampleMessageEncoder: Encoder[SampleMessage] = deriveEncoder
  implicit val sampleMessageDecoder: Decoder[SampleMessage] = deriveDecoder
}
