package com.openthoughts.ingest.client.upload;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
public class PreSignedUrlResponse {
  private URL preSignedUrl;
  private boolean success;
  private boolean retryLater;
  private boolean duplicateRequest;
  private String errorMessage;
  private int statusCode;

  public boolean isSuccess() {
    return success;
  }

  public boolean isDuplicateRequest() {
    return duplicateRequest;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isRetryLater() {
    return retryLater;
  }

  public URL getPreSignedUrl() {
    return preSignedUrl;
  }

  public PreSignedUrlResponse(final URL preSignedUrl,final int statusCode) {
    this.preSignedUrl = preSignedUrl;
    this.success = true;
    this.statusCode = statusCode;
    this.errorMessage = null;
    initialize(statusCode);
  }

  public PreSignedUrlResponse(final String errorMessage,final int statusCode) {
    this.success = false;
    this.preSignedUrl = null;
    this.statusCode = statusCode;
    this.errorMessage = errorMessage;
    initialize(statusCode);
  }

  public void initialize(final int statusCode) {
    if (statusCode == HttpStatus.SC_CONFLICT) {
      duplicateRequest = true;
      retryLater = false;
    } else {
      duplicateRequest = false;
      retryLater = true;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(PreSignedUrlResponse.class);
}