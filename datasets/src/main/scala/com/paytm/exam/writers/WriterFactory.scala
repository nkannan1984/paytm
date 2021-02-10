package com.paytm.exam.writers

import com.paytm.exam.exceptions.{WriteFailedException}
import com.paytm.exam.writers.file.CSVOutputWriter

object WriterFactory {

  def get(outputConfig: Output): Writer = {
    val outputOptions =
      outputConfig.outputOptions.asInstanceOf[Map[String, String]]
    val outputWriter = outputConfig.outputType match {
      case OutputType.CSV => new CSVOutputWriter(outputOptions)
      case _              => throw WriteFailedException(s"Not Supported Writer")
    }

    outputWriter

  }

  // scalastyle:on cyclomatic.complexity
}
