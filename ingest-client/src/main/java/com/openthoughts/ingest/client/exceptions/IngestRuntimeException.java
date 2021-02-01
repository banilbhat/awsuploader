package com.openthoughts.ingest.client.exceptions;


public class IngestRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public IngestRuntimeException(final String message,final Throwable cause) {
    super(message, cause);
  }

  public IngestRuntimeException(final String message) {
    super(message);
  }
}
