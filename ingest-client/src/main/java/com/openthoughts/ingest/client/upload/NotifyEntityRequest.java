package com.openthoughts.ingest.client.upload;


import com.openthoughts.ingest.client.Configuration;
import com.openthoughts.ingest.client.exceptions.IngestClientWebException;
import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;
import com.openthoughts.ingest.client.file.S3File;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
public class NotifyEntityRequest {
  private S3File s3File;
  private boolean update;

  // This is the code to call ingest-service to get the notify entity
  public NotifyEntityRequest(final S3File s3File, final boolean update) {
    this.s3File = s3File;
    this.update = update;
  }

  protected void notifyEntity() throws IngestClientWebException {
    try {
      final Client client = ClientBuilder.newClient();
      final IngestEntity ingestEntity = new IngestEntity(s3File.getFileHashKey(), s3File.getFileAbsolutePath(), s3File.getFileName());
      final Response response = client.target(Configuration.getIngestBaseUrl())
          .path(S3Upload.INGEST_ROOT_RESOURCE_PATH)
          .path(S3Upload.INGEST_NOTIFY_PATH)
          .queryParam("update", update)
          .request(MediaType.APPLICATION_JSON)
          .accept(MediaType.APPLICATION_JSON)
          .post(Entity.entity(ingestEntity, MediaType.APPLICATION_JSON));
      if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
        LOG.error("unable to notify ingestservice, filehashKey: {}, fileabsolutPath: {}, response code:{} , Error message: {}", s3File
            .getFileHashKey(), s3File.getFileAbsolutePath(), response.getStatus(), response.readEntity(String.class));
        s3File.setNotifySuccess(false);
        throw new IngestClientWebException(response.getStatus(), true, "unable to notify ingestservice, filehashKey:" + s3File
            .getFileHashKey() + " fileabsolutePath: " + s3File
            .getFileAbsolutePath());
      }
      s3File.setNotifySuccess(true);
    } catch (ProcessingException ex) {
      LOG.error("Ingest service is down. Service url:{}", Configuration.getIngestBaseUrl(), ex);
      throw new IngestRuntimeException("Ingest service is down. Service url: " + Configuration.getIngestBaseUrl(), ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(NotifyEntityRequest.class);
}
