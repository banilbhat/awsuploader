package com.openthoughts.ingest.client.upload;

import com.openthoughts.ingest.client.Configuration;
import com.openthoughts.ingest.client.exceptions.IngestClientWebException;
import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;
import com.openthoughts.ingest.client.file.S3File;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

public abstract class S3Upload {

  protected String folderPath;

  public S3Upload(final String folderPath) {
    this.folderPath = folderPath;
  }

  public abstract boolean uploadFile(final S3File s3File, final boolean deleteAfterUpload);

  private boolean retryPreviousFailures() {
    boolean continueNext = true;
    for (String filePath : LocalCache.getInstance().getRetrySet()) {
      LOG.info("Retry of file upload: {}", filePath);
      final S3File s3File = new S3File(filePath);
      if (!s3File.isInitializationSuccessful()) {
        LOG.warn("Initialization of file failed: {}", filePath);
        LocalCache.getInstance().removeFromRetryCache(s3File.getFileAbsolutePath());
        continue;
      }
      if (isUploaded(s3File)) {
        s3File.deleteFile(false);
        LOG.warn("Retry successful.");
      } else if (!uploadFile(s3File, false)) {
        LOG.error("Retry failed.");
        continueNext = false;
      }
      if (continueNext) {
        final String val = LocalCache.getInstance().get(s3File.getFileHashKey());
        if (val != null && val.equals("1,1")) {
          LocalCache.getInstance().removeFromRetryCache(s3File.getFileAbsolutePath());
        }
      } else {
        return continueNext;
      }
    }
    return true;
  }

  private boolean validate() {
    final File hotFolderFile = new File(folderPath);
    if (!hotFolderFile.exists()) {
      LOG.warn("The hotfolder doesnt exist.{}", folderPath);
      return false;
    }
    return true;
  }

  public static long getFileCount() {
    return fileCount;
  }

  public boolean uploadFolder(final boolean deleteAfterUpload) {

    try {
      retryPreviousFailures();
      fileCount = 0;

      LOG.info("Start of uploadfolder(), folderpath:{}", folderPath);
      if (!validate()) {
        return true;
      }
      Files.walkFileTree(FileSystems.getDefault().getPath(folderPath), new FileVisitor<Path>() {
        // Called after a directory visit is complete.
        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc)
            throws IOException {
          LOG.info("Parsing directory ended. dirName:{} ", dir.toAbsolutePath().toString());
          return FileVisitResult.CONTINUE;
        }

        // called before a directory visit.
        @Override
        public FileVisitResult preVisitDirectory(final Path dir,
            final BasicFileAttributes attrs) throws IOException {
          //check for last modification date and skip if not necessary
          LOG.info("Parsing directory started. dirName:{} ", dir.toAbsolutePath().toString());
          return FileVisitResult.CONTINUE;
        }

        // This method is called for each file visited. The basic attributes of the files are also available.
        @Override
        public FileVisitResult visitFile(final Path file,
            final BasicFileAttributes attrs) throws IOException {
          fileCount++;
          LOG.info("Last access time: " + attrs.lastAccessTime() + " modification time: " + attrs.lastModifiedTime() + file
              .toAbsolutePath()
              .toString());
          if (attrs.isSymbolicLink()) {
            return FileVisitResult.CONTINUE;
          }
          final S3File s3File = new S3File(file.toAbsolutePath().toString());
          if (!s3File.isInitializationSuccessful()) {
            LOG.warn("the file might have been moved out or hidden. fileName: " + s3File.getFileAbsolutePath());
            return FileVisitResult.CONTINUE;
          }
          LOG.info("File being visited to upload, fileName:{}", file.toAbsolutePath().toString());
          if (isUploaded(s3File)) {
            s3File.deleteFile(deleteAfterUpload);
            return FileVisitResult.CONTINUE;
          }
          if (uploadFile(s3File, deleteAfterUpload)) {
            return FileVisitResult.CONTINUE;
          } else {
            return FileVisitResult.TERMINATE;
          }
        }

        // if the file visit fails for any reason, the visitFileFailed method is called.
        @Override
        public FileVisitResult visitFileFailed(final Path file, final IOException ex)
            throws IOException {
          LOG.error("IO error parsing the file in directory, fileName:{}", file.getFileName(), ex);
          if (ex instanceof FileNotFoundException) {
            return FileVisitResult.CONTINUE;
          } else {
            return FileVisitResult.TERMINATE;
          }
        }
      });
      LOG.info("End of uploadfolder(),folderpath:{}, fileCount:{}", folderPath, fileCount);
      return true;
    } catch (IOException ex) {
      LOG.error("IO error reading file.", ex);
      return false;
    } finally {
      LocalCache.getInstance().commit(folderPath);
    }
  }

  public void updateUploadInfo(final S3File s3File) {

    if (s3File.isUploadSuccessful() && s3File.isNotifySuccess()) {
      LocalCache.getInstance().put(s3File.getFileHashKey(), "1,1", s3File.getFileAbsolutePath());
      LOG.info("Upload status of file: " + s3File.getFileAbsolutePath() + ":" + s3File.getUploadStatus() + ":" + s3File.isNotifySuccess());
      return;
    }
    LOG.error("Upload status of file: " + s3File.getFileAbsolutePath() + ":" + s3File.getUploadStatus() + ":" + s3File.isNotifySuccess());
    if (s3File.isUploadSuccessful() && !s3File.isNotifySuccess()) {
      LocalCache.getInstance().put(s3File.getFileHashKey(), "1,0", s3File.getFileAbsolutePath());
      LocalCache.getInstance().addToRetrySet(s3File.getFileAbsolutePath());
      return;
    }
    if (!s3File.isNotifySuccess()) {
      LocalCache.getInstance().put(s3File.getFileHashKey(), "0,0", s3File.getFileAbsolutePath());
      LocalCache.getInstance().addToRetrySet(s3File.getFileAbsolutePath());
    } else {
      LocalCache.getInstance().put(s3File.getFileHashKey(), "0,1", s3File.getFileAbsolutePath());
      LocalCache.getInstance().addToRetrySet(s3File.getFileAbsolutePath());
    }
  }




  private boolean isUploaded(final S3File s3File) {
    if (LocalCache.getInstance().contains(s3File.getFileHashKey())) {
      final String statusValue = LocalCache.getInstance().get(s3File.getFileHashKey());
      final String[] flag = statusValue.split(",");
      s3File.setNotifySuccess(flag[1].equals("1"));
      if (flag[0].equals("1")) {
        LOG.info("File is already uploaded.fileName: {}", s3File.getFileAbsolutePath());
        s3File.setUploadStatus(HttpStatus.SC_OK);
        if (flag[1].equals("0")) {
          try {
            LOG.info("Retry Notify for file hashkey: {}, fileName: {}", s3File.getFileHashKey(), s3File.getFileAbsolutePath());
            NotifyEntityRequest notifyEntityRequest = new NotifyEntityRequest(s3File,true);
            notifyEntityRequest.notifyEntity();
            updateUploadInfo(s3File);
          } catch (IngestClientWebException e) {
            // just log, do nothing
            LOG.error("Error retrying to notify");
          }
        }
        return true;
      }
    }
    return false;
  }

  private String getFileExtension(final File file) {
    final String name = file.getName();
    final int index = name.lastIndexOf(".");
    if (index != -1) {
      return name.substring(index);
    } else {
      return "";
    }
  }


  private static long fileCount = 0;
  private static final Logger LOG = LoggerFactory.getLogger(S3Upload.class);

  protected static final String PRESIGNED_URL_RESOURCE_PATH = "preSignedUrl";
  protected static final String AUTH_RESOURCE_PATH = "authenticatedToken";
  public static final String INGEST_ROOT_RESOURCE_PATH = "ingest";
  protected static final String INGEST_NOTIFY_PATH = "notify";
  protected static final String FILE_HASH_KEY_PARAM_NAME = "fileHashKey";
  protected final long MULTI_PART_INDIVIDUAL_PART_SIZE = 5242880;// Set part size to 5 MB.

}
