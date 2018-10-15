package com.earnest.util.aws

import com.earnest.util.aws.s3.syntax.AllSyntax

package object s3 {
  type Error = String

  object connection extends Connection
  object implicits extends AllSyntax
}
