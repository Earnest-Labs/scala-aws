package com.earnest.util.aws.s3.syntax

import com.earnest.util.aws.s3.syntax.query._
import cats.effect.Effect
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import cats.instances.list._
import com.earnest.util.aws.s3.S3DataSource

import scala.language.implicitConversions

final class DeleteOps[F[_]](val s3DS: S3DataSource[F]) extends AnyVal {
  def deleteFile(key: String)(implicit F: Effect[F]): F[Unit] =
    s3DS.eval(F.delay(s3DS.tfm.getAmazonS3Client.deleteObject(s3DS.s3Conf.bucket, key)))

  def deleteFilesInDir(dir: String)(implicit F: Effect[F]): F[Unit] =
    s3DS.eval(s3DS.listFileMetadataInDir(dir) >>= (_.traverse(meta => deleteFile(meta.getKey)).void))

  def deleteFiles(fileNames: List[String])(implicit F: Effect[F]): F[Unit] =
    s3DS.eval(F.delay(fileNames.foreach(fileName => deleteFile(fileName))))
}

trait ToDeleteOps {
  implicit def toDeleteOps[F[_]](s3DS: S3DataSource[F]): DeleteOps[F] =
    new DeleteOps(s3DS)
}

object delete extends ToDeleteOps
