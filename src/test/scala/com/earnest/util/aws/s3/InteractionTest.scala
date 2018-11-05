package com.earnest.util.aws.s3

import java.io.ByteArrayInputStream
import java.util.concurrent.Executors

import com.earnest.util.aws.s3.implicits._
import com.earnest.util.aws.s3
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import cats.effect.IO
import cats.syntax.flatMap._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import scala.concurrent.ExecutionContext

object s3Env {
  val tp = Executors.newFixedThreadPool(2)
  val blockingEc = ExecutionContext.fromExecutor(tp)
  implicit val cs = IO.contextShift(ExecutionContext.global)
  val s3DS = (config.getFromEnvironment[IO]() >>= (conf => s3.connection.createS3DataSource[IO](conf, blockingEc))).unsafeRunSync()
}

final class InteractionTest extends FreeSpec with Matchers with BeforeAndAfterAll {
  import s3Env._

  override def afterAll() = {
    s3DS.tfm.shutdownNow()
    tp.shutdownNow()
    ()
  }

  s"S3 client" - {
    val dir = "share"

    "should be able to upload to and download from S3 server" in {

      s3DS.deleteFilesInDir(dir).unsafeRunSync()

      val goodCatContent = "I am a good, meowing cat"
      val badCatContent = "I am a bad, meowing cat"
      val goodCatFileName = s"$dir/goodCat"

      s3DS.doesFileExist(goodCatFileName).unsafeRunSync() shouldBe false

      val goodCatBytes = goodCatContent.getBytes()
      s3DS.upload(goodCatFileName, new ByteArrayInputStream(goodCatBytes), goodCatBytes.length)
        .unsafeRunSync()

      s3DS.doesFileExist(goodCatFileName).unsafeRunSync() shouldBe true

      // Makes sure badCat is uploaded last. Otherwise, that guarantee is not possible due to S3/minio being eventually consistent
      Thread.sleep(1000)

      val badCatBytes = badCatContent.getBytes()
      val badCatFilename = s"$dir/badCat"

      s3DS.doesFileExist(badCatFilename).unsafeRunSync() shouldBe false

      s3DS.upload(badCatFilename, new ByteArrayInputStream(badCatBytes), badCatBytes.length)
        .unsafeRunSync()

      s3DS.doesFileExist(badCatFilename).unsafeRunSync() shouldBe true

      val lastUploadedFileMeta = s3DS.getLastUploadedFileMetaInDir(dir).unsafeRunSync()
      lastUploadedFileMeta.map(_.getKey).getOrElse("badLocation") shouldBe s"$dir/badCat"

      s3DS.listFileMetadataInDir(dir).unsafeRunSync().size shouldBe 2
    }

    "should be able upload and download JSON objects" in {
      val key = "meta"
      val meta = Meta(true)
      val prevJob1 = (s3DS.upsertJson[Meta](key, meta) >> s3DS.getJson[Meta](key)).unsafeRunSync()

      prevJob1 shouldBe Some(meta)

      val meta2 = Meta(false)
      val prevJob2 = (s3DS.upsertJson[Meta](key, meta2) >> s3DS.getJson[Meta](key)).unsafeRunSync()

      prevJob2 shouldBe Some(meta2)
    }
  }
}
final case class Meta(open: Boolean)
object Meta {
  implicit val jobRunMetaEncoder: Encoder[Meta] = deriveEncoder
  implicit val jobRunMetaDecoder: Decoder[Meta] = deriveDecoder
}
