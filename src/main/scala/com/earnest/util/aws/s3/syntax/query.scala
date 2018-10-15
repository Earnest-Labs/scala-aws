package com.earnest.util.aws.s3.syntax

import com.amazonaws.services.s3.model.{ListObjectsV2Result, S3ObjectSummary}
import com.earnest.util.aws.s3.S3DataSource

import scala.collection.JavaConverters._
import cats.effect.Effect
import cats.syntax.functor._

import scala.language.implicitConversions

final class QueryOps(val s3DS: S3DataSource) extends AnyVal {
  def doesFileExist[F[_]](key: String)(implicit F: Effect[F]): F[Boolean] =
    F.delay(s3DS.tfm.getAmazonS3Client.doesObjectExist(s3DS.s3Conf.bucket, key))

  def listDir[F[_]](dir: String)(implicit F: Effect[F]): F[ListObjectsV2Result] =
    F.delay(s3DS.tfm.getAmazonS3Client.listObjectsV2(s3DS.s3Conf.bucket, dir))

  def listFileMetadataInDir[F[_]](dir: String)(implicit F: Effect[F]): F[List[S3ObjectSummary]] =
    listDir(dir).map(_.getObjectSummaries.asScala.toList)

  def getLastUploadedFileMetaInDir[F[_]](dir: String)(implicit F: Effect[F]): F[Option[S3ObjectSummary]] =
    listFileMetadataInDir(dir).map(_.sortBy(_.getLastModified.getTime).lastOption)
}

trait ToQueryOps {
  implicit def toQueryOps(s3DS: S3DataSource): QueryOps =
    new QueryOps(s3DS)
}

object query extends ToQueryOps
