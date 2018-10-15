package com.earnest.util.aws.s3.syntax

trait AllSyntax
  extends ToTransferOps
  with ToQueryOps
  with ToDeleteOps
  with ToJsonOps

object all extends AllSyntax
