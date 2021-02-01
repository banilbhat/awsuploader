package com.openthoughts.ingest.client;

import com.openthoughts.ingest.client.upload.PreSignedUrlS3Upload;
import com.openthoughts.ingest.client.upload.S3Upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

  public static void main(String[] args) {
    try {
      if (!AppLock.setLock("INGEST_CLIENT_LOCK_KEY")) {
        LOG.warn("Only one application instance may run at the same time!");
        System.exit(0);
      }
      if (args.length < 1) {
        LOG.error("Please enter the path for hotfolder.");
        return;
      }
      Configuration.initialize(APPLICATION_NAME);
      final String folderPath = args[0];
      final S3Upload algo = new PreSignedUrlS3Upload(folderPath);
      algo.uploadFolder(Configuration.isDeleteAfterUpload());
    } catch (Exception e) {
      LOG.error("Application exiting because of errors", e);
      System.exit(1);
    } finally {
      System.exit(0);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);
  private static final String APPLICATION_NAME = "dam-ingest-client";
}
