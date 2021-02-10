package com.paytm.exam.exceptions

case class ProcessingException(private val message: String = "",
                               private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
