package com.openthoughts.ingest.client.exceptions;


public class IngestClientWebException extends Exception {
  private int statusCode;
  private boolean notifyFail;

  public IngestClientWebException(final int statusCode,final boolean notifyFail,final String message) {
    super(message);
    this.statusCode = statusCode;
    this.notifyFail = notifyFail;
  }

  public boolean isNotifyFail() {
    return notifyFail;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
