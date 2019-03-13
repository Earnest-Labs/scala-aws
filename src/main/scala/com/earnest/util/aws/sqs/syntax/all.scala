package com.earnest.util.aws.sqs.syntax

trait AllSyntax
  extends ToDeliveryOps
  with ToQueryOps
  with ToReceptionOps

object all extends AllSyntax
