package com.openthoughts.ingest.client.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class FederatedToken {
  private final String secretAccessKey;
  private final String accessKeyId;
  private final String sessionToken;

  @JsonCreator
  public FederatedToken(@JsonProperty(ACCESSKEYID) final String accessKeyId,
      @JsonProperty(SECRETACCESSKEY) final String secretAccessKey,
      @JsonProperty(SESSIONTOKEN) final String sessionToken) {
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.accessKeyId = accessKeyId;
  }

  @JsonProperty(SECRETACCESSKEY)
  public String secretAccessKey() {
    return secretAccessKey;
  }

  @JsonProperty(SESSIONTOKEN)
  public String sessionToken() {
    return sessionToken;
  }

  @JsonProperty(ACCESSKEYID)
  public String accessKeyId() {
    return accessKeyId;
  }

  private static final String SECRETACCESSKEY = "secretAccessKey";
  private static final String SESSIONTOKEN = "sessionToken";
  private static final String ACCESSKEYID = "accessKeyId";
}


