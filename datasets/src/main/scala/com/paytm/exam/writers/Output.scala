package com.paytm.exam.writers

case class Output(name: Option[String],
                  dataFrameName: String,
                  outputType: OutputType.OutputType,
                  repartition: Option[Int],
                  coalesce: Option[Boolean],
                  protectFromEmptyOutput: Option[Boolean],
                  outputOptions: Map[String, Any])
