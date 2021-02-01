package com.openthoughts.ingest.client.upload;

import com.openthoughts.ingest.client.Configuration;
import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
public class PreSignedUrlRequest {
  private String fileHashKey;

  public PreSignedUrlRequest(final String fileHashKey) {
    this.fileHashKey = fileHashKey;
  }

  // This is the code to call ingest-service to get the presigned url
  // credentials to upload to S3
  public PreSignedUrlResponse generatePresignedUrl() {
    try {
      final Client client = ClientBuilder.newClient();
      final Response response = client.target(Configuration.getIngestBaseUrl())
          .path(S3Upload.INGEST_ROOT_RESOURCE_PATH)
          .path(S3Upload.PRESIGNED_URL_RESOURCE_PATH).queryParam(S3Upload.FILE_HASH_KEY_PARAM_NAME, fileHashKey)
          .request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).get();
      final int statusCode = response.getStatus();
      final Family statusFamily = response.getStatusInfo().getFamily();
      if (statusFamily == Family.SUCCESSFUL) {
        final URL preSignedUrl = response.readEntity(URL.class);
        LOG.debug("HTTP Response, APIResourcePath : {} , statusCode: {}",
            Configuration.getIngestBaseUrl() + "/" + S3Upload.PRESIGNED_URL_RESOURCE_PATH, statusCode);
        return new PreSignedUrlResponse(preSignedUrl, statusCode);
      } else if (statusFamily == Family.SERVER_ERROR && statusCode == HttpStatus.SC_CONFLICT) {
        final String errorResponse = response.readEntity(String.class);
        LOG.error("Duplicate request.HTTP Response, APIResourcePath : {} , statusCode: {}, error: {}",
            Configuration.getIngestBaseUrl() + "/" + S3Upload.PRESIGNED_URL_RESOURCE_PATH, statusCode, errorResponse);
        return new PreSignedUrlResponse(errorResponse, statusCode);
      } else {
        final String errorResponse = response.readEntity(String.class);
        LOG.error("HTTP Response error, APIResourcePath : {} , statusCode: {}, error: {}",
            Configuration.getIngestBaseUrl() + "/" + S3Upload.PRESIGNED_URL_RESOURCE_PATH, statusCode, errorResponse);
        return new PreSignedUrlResponse(errorResponse, statusCode);
      }
    } catch (ProcessingException ex) {
      LOG.error("Ingest service is down. Service url:{}", Configuration.getIngestBaseUrl(), ex);
      throw new IngestRuntimeException("Ingest service is down.Service Url:" + Configuration.getIngestBaseUrl(), ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(PreSignedUrlRequest.class);
}
