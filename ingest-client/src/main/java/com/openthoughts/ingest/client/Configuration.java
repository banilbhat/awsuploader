package com.openthoughts.ingest.client;

import com.openthoughts.ape.dev.common.config.DynamicVariableLookup;
import com.openthoughts.ape.dev.common.config.DynamicVariableSubstitutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration {
  private static String ingestBaseUrl;
  private static String s3bucketPrimary;
  private static boolean deleteAfterUpload;

  public static String getIngestBaseUrl() {
    return ingestBaseUrl;
  }

  public static void setIngestBaseUrl(final String ingestBaseUrl) {
    Configuration.ingestBaseUrl = ingestBaseUrl;
  }

  public static String getS3bucketPrimary() {
    return s3bucketPrimary;
  }

  public static void setS3bucketPrimary(final String s3bucketPrimary) {
    Configuration.s3bucketPrimary = s3bucketPrimary;
  }

  public static boolean isDeleteAfterUpload() {
    return deleteAfterUpload;
  }

  public static void setDeleteAfterUpload(final boolean deleteAfterUpload) {
    Configuration.deleteAfterUpload = deleteAfterUpload;
  }

  public static void initialize(final String applicationName) {
    final DynamicVariableSubstitutor dvs = new DynamicVariableSubstitutor(applicationName, true, false);
    final DynamicVariableLookup dynamicVariableLookup = (DynamicVariableLookup) dvs.getVariableResolver();
    s3bucketPrimary = dynamicVariableLookup.lookup("S3_BUCKET_PRIMARY");
    ingestBaseUrl = dynamicVariableLookup.lookup("INGEST_SERVICE_URL");
    deleteAfterUpload = Boolean.parseBoolean(dynamicVariableLookup.lookup("DELETE_AFFTER_UPLOAD"));
  }

  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);


}
