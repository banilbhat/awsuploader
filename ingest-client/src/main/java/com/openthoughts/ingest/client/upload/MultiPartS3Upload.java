package com.openthoughts.ingest.client.upload;

import com.openthoughts.ingest.client.Configuration;
import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;
import com.openthoughts.ingest.client.file.S3File;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

public class MultiPartS3Upload extends S3Upload {


  public MultiPartS3Upload(final String folderPath) {
    super(folderPath);
  }

  @Override
  public boolean uploadFile(final S3File s3File, final boolean deleteAfterUpload) {
    try {
      final AmazonS3Client s3Client = buildCredentials();
      final boolean continueNext = uploadFile(s3Client, s3File);
      updateUploadInfo(s3File);
      return continueNext;
    } finally {
      s3File.deleteFile(deleteAfterUpload);
    }
  }


  // This is the code to call ingest-service to get the federatedtoken
  // credentials to upload to S3
  public AmazonS3Client buildCredentials() {
    try {
      final Client client = ClientBuilder.newClient();
      FederatedToken federatedToken = null;
      final Response response = client.target(Configuration.getIngestBaseUrl())
          .path(INGEST_ROOT_RESOURCE_PATH)
          .path(AUTH_RESOURCE_PATH).request(MediaType.APPLICATION_JSON)
          .accept(MediaType.APPLICATION_JSON).get();
      final int statusCode = response.getStatus();
      if (response.getStatusInfo().getFamily() == Family.SUCCESSFUL) {
        federatedToken = response.readEntity(FederatedToken.class);
        LOG.debug("HTTP Response, APIResourcePath : {} , statusCode: {}",
            Configuration.getIngestBaseUrl() + AUTH_RESOURCE_PATH, statusCode);
      } else {
        LOG.error("HTTP Response, APIResourcePath : {} , statusCode: {}, error: {}",
            Configuration.getIngestBaseUrl() + AUTH_RESOURCE_PATH, statusCode);
        throw new IngestRuntimeException("Cannot access Ingest service to fetch S3 credentials");
      }
      final BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(
          federatedToken.accessKeyId(), federatedToken.secretAccessKey(), federatedToken.sessionToken());
      return new AmazonS3Client(basicSessionCredentials);
    } catch (ProcessingException ex) {
      LOG.error("Ingest service is down. Service url:{}", Configuration.getIngestBaseUrl(), ex);
      throw new IngestRuntimeException("Ingest service is down.Service Url:" + Configuration.getIngestBaseUrl(), ex);
    }
  }

  public boolean uploadFile(final AmazonS3Client s3Client, final S3File s3file) {
    final List<PartETag> partETags = new ArrayList<PartETag>();
    final String filePath = s3file.getFileHashKey();
    final File file = new File(filePath);
    final String keyName = s3file.getFileHashKey();
    final long contentLength = file.length();
    long partSize = MULTI_PART_INDIVIDUAL_PART_SIZE;
    // Step 1: Initialize.
    final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(Configuration.getS3bucketPrimary(),
        keyName);
    final InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    try {
      // Step 2: Upload parts.
      long filePosition = 0;
      for (int i = 1; filePosition < contentLength; i++) {
        // Last part can be less than 5 MB. Adjust part size.
        partSize = Math.min(partSize, contentLength - filePosition);
        // Create request to upload a part.
        final UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(Configuration.getS3bucketPrimary())
            .withKey(keyName).withUploadId(initResponse.getUploadId()).withPartNumber(i)
            .withFileOffset(filePosition).withFile(file).withPartSize(partSize);

        // Upload part and add response to our list.
        partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
        filePosition += partSize;
      }
      // Step 3: Complete.
      final CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(Configuration.getS3bucketPrimary(),
          keyName, initResponse.getUploadId(), partETags);

      s3Client.completeMultipartUpload(compRequest);
      return true;
    } catch (AmazonServiceException e) {
      s3Client.abortMultipartUpload(
          new AbortMultipartUploadRequest(Configuration.getS3bucketPrimary(), keyName, initResponse.getUploadId()));
      LOG.error("Error: Upload FilePath: {}", filePath, e);
      return false;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(MultiPartS3Upload.class);

}
