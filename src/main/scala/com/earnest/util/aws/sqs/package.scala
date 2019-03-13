package com.earnest.util.aws

import com.earnest.util.aws.sqs.syntax.AllSyntax

package object sqs {
  object connection extends Connection
  object implicits extends AllSyntax
}
