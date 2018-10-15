package com.earnest.util.aws.s3

import java.io.ByteArrayInputStream

import com.earnest.util.aws.s3.implicits._
import com.earnest.util.aws.s3
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import cats.effect.IO
import cats.syntax.flatMap._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

object s3Env {
  val s3DS = (config.getFromEnvironment[IO]() >>= s3.connection.createS3DataSource[IO]).unsafeRunSync()
}

final class InteractionTest extends FreeSpec with Matchers with BeforeAndAfterAll {
  import s3Env._

  override def afterAll() = {
    s3DS.tfm.shutdownNow()
  }

  s"S3 client" - {
    val dir = "share"

    "should be able to upload to and download from S3 server" in {

      s3DS.deleteFilesInDir[IO](dir).unsafeRunSync()

      val goodCatContent = "I am a good, meowing cat"
      val badCatContent = "I am a bad, meowing cat"
      val goodCatFileName = s"$dir/goodCat"

      s3DS.doesFileExist[IO](goodCatFileName).unsafeRunSync() shouldBe false

      val goodCatBytes = goodCatContent.getBytes()
      s3DS.upload[IO](goodCatFileName, new ByteArrayInputStream(goodCatBytes), goodCatBytes.length)
        .unsafeRunSync()

      s3DS.doesFileExist[IO](goodCatFileName).unsafeRunSync() shouldBe true

      // Makes sure badCat is uploaded last. Otherwise, that guarantee is not possible due to S3/minio being eventually consistent
      Thread.sleep(1000)

      val badCatBytes = badCatContent.getBytes()
      val badCatFilename = s"$dir/badCat"

      s3DS.doesFileExist[IO](badCatFilename).unsafeRunSync() shouldBe false

      s3DS.upload[IO](badCatFilename, new ByteArrayInputStream(badCatBytes), badCatBytes.length)
        .unsafeRunSync()

      s3DS.doesFileExist[IO](badCatFilename).unsafeRunSync() shouldBe true

      val lastUploadedFileMeta = s3DS.getLastUploadedFileMetaInDir[IO](dir).unsafeRunSync()
      lastUploadedFileMeta.map(_.getKey).getOrElse("badLocation") shouldBe s"$dir/badCat"

      s3DS.listFileMetadataInDir[IO](dir).unsafeRunSync().size shouldBe 2
    }

    "should be able upload and download JSON objects" in {
      val key = "meta"
      val meta = Meta(true)
      val prevJob1 = (s3DS.upsertJson[IO, Meta](key, meta) >> s3DS.getJson[IO, Meta](key)).unsafeRunSync()

      prevJob1 shouldBe Some(meta)

      val meta2 = Meta(false)
      val prevJob2 = (s3DS.upsertJson[IO, Meta](key, meta2) >> s3DS.getJson[IO, Meta](key)).unsafeRunSync()

      prevJob2 shouldBe Some(meta2)
    }
  }
}
final case class Meta(open: Boolean)
object Meta {
  implicit val jobRunMetaEncoder: Encoder[Meta] = deriveEncoder
  implicit val jobRunMetaDecoder: Decoder[Meta] = deriveDecoder
}
