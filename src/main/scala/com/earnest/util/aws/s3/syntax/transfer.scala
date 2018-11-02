package com.earnest.util.aws.s3.syntax

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.transfer.model.UploadResult
import com.earnest.util.aws.s3.S3DataSource
import cats.effect.Effect
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.instances.list._
import com.earnest.util.aws.s3.syntax.query._

import scala.language.implicitConversions

final class TransferOps[F[_]](val s3DS: S3DataSource[F]) extends AnyVal {
  def upload(key: String, is: InputStream, contentLength: Long)
    (implicit F: Effect[F]): F[UploadResult] =
    s3DS.CS.evalOn(s3DS.blockingEc)(F.delay {
      val metadata = new ObjectMetadata()
      metadata.setContentLength(contentLength)
      s3DS.tfm.upload(s3DS.s3Conf.bucket, key, is, metadata).waitForUploadResult()
    })

  /**
    * Useful if you are going to be reading the file line by line and you don't want to deal with
    * buffering
    */
  def openBufferedReaderToFile(fileName: String)(implicit F: Effect[F]): F[BufferedReader] =
    s3DS.CS.evalOn(s3DS.blockingEc)(openStreamToFile(fileName).map(is => new BufferedReader(new InputStreamReader(is))))

  /**
    * Useful if you want the stream buffered
    */
  def openBufferedStreamToFile(fileName: String)(implicit F: Effect[F]): F[InputStream] =
    s3DS.CS.evalOn(s3DS.blockingEc)(openStreamToFile(fileName).map(is => new BufferedInputStream(is)))

  /**
    * Use if you want to manage the interpretation of the stream and the buffering of it yourself
    */
  def openStreamToFile(key: String)(implicit F: Effect[F]): F[InputStream] =
    s3DS.CS.evalOn(s3DS.blockingEc)(F.delay(s3DS.tfm.getAmazonS3Client.getObject(s3DS.s3Conf.bucket, key).getObjectContent))

  def openStreamsToFilesInDir(dir: String)(implicit F: Effect[F]): F[List[InputStream]] =
    s3DS.CS.evalOn(s3DS.blockingEc)(s3DS.listFileMetadataInDir(dir) >>= (_.traverse(meta => openStreamToFile(meta.getKey))))

  def openStreamsToFiles(files: List[String])
    (implicit F: Effect[F]): F[List[InputStream]] = s3DS.CS.evalOn(s3DS.blockingEc)(files.traverse(openStreamToFile(_)))
}

trait ToTransferOps {
  implicit def toTransferOps[F[_]](s3DS: S3DataSource[F]): TransferOps[F] =
    new TransferOps(s3DS)
}

object transfer extends ToTransferOps
