package com.earnest.util

import io.circe.Printer

package object aws {
  val jsonPrinter = Printer.noSpaces.copy(dropNullValues = true)
}
