package com.earnest.util.aws.s3.syntax

import com.amazonaws.services.s3.model.{ListObjectsV2Result, S3ObjectSummary}
import com.earnest.util.aws.s3.S3DataSource

import scala.collection.JavaConverters._
import cats.effect.Effect
import cats.syntax.functor._

import scala.language.implicitConversions

final class QueryOps[F[_]](val s3DS: S3DataSource[F]) extends AnyVal {
  def doesFileExist(key: String)(implicit F: Effect[F]): F[Boolean] =
    s3DS.cs.evalOn(s3DS.blockingEc)(F.delay(s3DS.tfm.getAmazonS3Client.doesObjectExist(s3DS.s3Conf.bucket, key)))

  def listDir(dir: String)(implicit F: Effect[F]): F[ListObjectsV2Result] =
    s3DS.cs.evalOn(s3DS.blockingEc)(F.delay(s3DS.tfm.getAmazonS3Client.listObjectsV2(s3DS.s3Conf.bucket, dir)))

  def listFileMetadataInDir(dir: String)(implicit F: Effect[F]): F[List[S3ObjectSummary]] =
    s3DS.cs.evalOn(s3DS.blockingEc)(listDir(dir).map(_.getObjectSummaries.asScala.toList))

  def getLastUploadedFileMetaInDir(dir: String)(implicit F: Effect[F]): F[Option[S3ObjectSummary]] =
    s3DS.cs.evalOn(s3DS.blockingEc)(listFileMetadataInDir(dir).map(_.sortBy(_.getLastModified.getTime).lastOption))
}

trait ToQueryOps {
  implicit def toQueryOps[F[_]](s3DS: S3DataSource[F]): QueryOps[F] =
    new QueryOps(s3DS)
}

object query extends ToQueryOps
