package com.paytm.exam.writers

import org.apache.spark.sql.DataFrame

trait Writer extends Serializable {
  private var mandatoryArguments = Seq[String]()

  def write(dataFrame: DataFrame): Unit

  def validateMandatoryArguments(outputOptions: Map[String, String]): Unit = {
    this.mandatoryArguments.foreach { arg =>
      if (!outputOptions.contains(arg))
        throw new Exception(
          s"Missing argument $arg for writer ${this.getClass.toString}"
        )
    }
  }

  protected def setMandatoryArguments(mandatoryArgs: String*): Unit = {
    this.mandatoryArguments = mandatoryArgs
  }

  case class MissingWriterArgumentException(private val message: String = "",
                                            private val cause: Throwable =
                                              None.orNull)
      extends Exception(message, cause)

}
