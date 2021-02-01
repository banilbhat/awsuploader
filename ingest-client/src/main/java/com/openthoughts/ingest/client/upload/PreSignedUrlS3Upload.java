package com.openthoughts.ingest.client.upload;

import com.openthoughts.ingest.client.exceptions.IngestClientWebException;
import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;
import com.openthoughts.ingest.client.file.S3File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class PreSignedUrlS3Upload extends S3Upload {

  public PreSignedUrlS3Upload(final String folderPath) {
    super(folderPath);
  }


  public boolean uploadFile(final S3File s3File, final boolean deleteAfterUpload) {
    LOG.info("uploadfile start, fileName:{}", s3File.getFileAbsolutePath());
    try {
      final PreSignedUrlResponse preSignedUrlResponse = generatePresignedUrl(s3File);
      boolean continueNext = true;
      if (preSignedUrlResponse.isSuccess()) {
        uploadFile(s3File, preSignedUrlResponse.getPreSignedUrl());
        continueNext = s3File.isUploadSuccessful();
        updateUploadInfo(s3File);
      } else if (preSignedUrlResponse.isDuplicateRequest()) {
        LocalCache.getInstance().put(s3File.getFileHashKey(), "1,1", s3File.getFileAbsolutePath());
        LOG.error("Cannot upload file. File already uploaded. Errormessage: " + preSignedUrlResponse.getErrorMessage());
        continueNext = true;
      } else if (preSignedUrlResponse.isRetryLater()) {
        LOG.error("Upload of files stopped. Because of upload error. Errormessage: " + preSignedUrlResponse.getErrorMessage());
        continueNext = false;
      }
      return continueNext;
    } finally {
      s3File.deleteFile(deleteAfterUpload);
    }
  }

  private void uploadFile(final S3File s3File, final URL preSignedUrl) {
    try {
      if (s3File.isNotifySuccess()) {
        s3File.setPreSignedUrl(preSignedUrl);
        s3File.uploadFilewithPreSignedUrl();
      } else {
        final Thread uploadFileThread = new Thread(s3File);
        s3File.setPreSignedUrl(preSignedUrl);
        uploadFileThread.start();
        final NotifyEntityRequest notifyEntityRequest = new NotifyEntityRequest(s3File,false);
        notifyEntityRequest.notifyEntity();
        uploadFileThread.join();
      }
    } catch (InterruptedException ex) {
      //handle case when uploadfile thread fails. delete the entity from entity store
      LOG.error("Error execution of uploadThread", ex);
      throw new IngestRuntimeException("Error execution of uploadThread", ex);
    } catch (IngestClientWebException ex) {
      // handle case when notify call was unsuccessful
      LOG.error("Error in notify call, notify status code:{}", ex.getStatusCode(), ex);
      if (ex.isNotifyFail()) {
        s3File.setNotifySuccess(false);
      }
    }
  }


  public PreSignedUrlResponse generatePresignedUrl(final S3File s3File) {
    final PreSignedUrlRequest preSignedUrlRequest = new PreSignedUrlRequest(s3File.getFileHashKey());
    final PreSignedUrlResponse preSignedUrlResponse = preSignedUrlRequest.generatePresignedUrl();
    return preSignedUrlResponse;
  }

  private static final Logger LOG = LoggerFactory.getLogger(PreSignedUrlS3Upload.class);
}
