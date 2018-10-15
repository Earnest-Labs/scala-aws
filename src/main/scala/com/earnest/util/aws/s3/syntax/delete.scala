package com.earnest.util.aws.s3.syntax

import com.earnest.util.aws.s3.syntax.query._
import cats.effect.Effect
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.instances.list._
import com.earnest.util.aws.s3.S3DataSource

import scala.language.implicitConversions

final class DeleteOps(val s3DS: S3DataSource) extends AnyVal {
  def deleteFile[F[_]](key: String)(implicit F: Effect[F]): F[Unit] =
    F.delay(s3DS.tfm.getAmazonS3Client.deleteObject(s3DS.s3Conf.bucket, key))

  def deleteFilesInDir[F[_]](dir: String)(implicit F: Effect[F]): F[Unit] =
    s3DS.listFileMetadataInDir(dir) >>= (_.traverse(meta => deleteFile(meta.getKey)).void)

  def deleteFiles[F[_]](fileNames: List[String])(implicit F: Effect[F]): F[Unit] =
    F.delay(fileNames.foreach(fileName => deleteFile(fileName)))
}

trait ToDeleteOps {
  implicit def toDeleteOps(s3DS: S3DataSource): DeleteOps =
    new DeleteOps(s3DS)
}

object delete extends ToDeleteOps
