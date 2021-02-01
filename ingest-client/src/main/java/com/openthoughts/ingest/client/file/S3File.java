package com.openthoughts.ingest.client.file;

import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.ws.rs.core.Response.Status.Family;
public class S3File implements Runnable {

  private final int FILE_CHUNK_SIZE = 4096;
  private String fileHashKey;
  private String fileAbsolutePath;
  private URL preSignedUrl;
  private int uploadStatus;
  private boolean notifySuccess;
  private boolean initializationSuccessful;


  public S3File(final String fileAbsolutePath) {
    this.fileAbsolutePath = fileAbsolutePath;
    this.notifySuccess = false;
    final File file = new File(fileAbsolutePath);
    if (file.exists() && isUploadable(file)) {
      fileHashKey = fetchFileHashKey(file);
      cacheFile();
      initializationSuccessful = true;
    } else {
      // file was moved at runtime or hidden
      initializationSuccessful = false;
    }
  }

  public boolean isInitializationSuccessful() {
    return initializationSuccessful;
  }

  public boolean isNotifySuccess() {
    return notifySuccess;
  }

  public void setNotifySuccess(final boolean notifySuccess) {
    this.notifySuccess = notifySuccess;
  }

  public int getUploadStatus() {
    return uploadStatus;
  }

  public boolean isUploadSuccessful() {
    return uploadStatus != 0 && Family.familyOf(uploadStatus) == Family.SUCCESSFUL;
  }

  public void setUploadStatus(final int uploadStatus) {
    this.uploadStatus = uploadStatus;
  }

  public void setPreSignedUrl(final URL preSignedUrl) {
    this.preSignedUrl = preSignedUrl;
  }

  public String getFileHashKey() {
    return fileHashKey;
  }

  public String getFileAbsolutePath() {
    return fileAbsolutePath;
  }

  public boolean deleteFile(final boolean deleteAfterUpload) {
    try {
      boolean deleteSuccess = true;
      if (deleteAfterUpload) {
        final File file = new File(fileAbsolutePath);
        if (isUploadSuccessful() && file.exists()) {
          deleteSuccess = file.delete();
        }
      }
      return deleteSuccess && clean();
    } catch (SecurityException ex) {
      LOG.error("Error while deleting the file:{}", fileAbsolutePath, ex);
      throw new IngestRuntimeException("Error while trying to delete the file", ex);
    }
  }

  private boolean isUploadable(final File file) {
    return !file.isHidden();
  }


  public String getFileName() {
    final Path path = Paths.get(fileAbsolutePath);
    if (path != null && path.getFileName() != null) {
      return path.getFileName().toString();
    } else {
      return null;
    }
  }

  private String fetchFileHashKey(final File file) {
    final StringBuffer sb = new StringBuffer();
    try {

      final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      final FileInputStream fileInputStream = new FileInputStream(file);
      final byte[] dataBytes = new byte[1024];
      try {
        int noofBytesRead = 0;
        while ((noofBytesRead = fileInputStream.read(dataBytes)) != -1) {
          messageDigest.update(dataBytes, 0, noofBytesRead);
        }
        final byte[] mdbytes = messageDigest.digest();
        for (int i = 0; i < mdbytes.length; i++) {
          sb.append(Integer.toString(mdbytes[i] & 0xff + 0x100, 16).substring(1));
        }
        return sb.toString();
      } finally {
        if (fileInputStream != null) {
          fileInputStream.close();
        }
      }
    } catch (FileNotFoundException ex) {
      LOG.error("File not found.file:{}", fileAbsolutePath, ex);
    } catch (IOException ex) {
      LOG.error("Error reading File :{}", fileAbsolutePath, ex);
    } catch (NoSuchAlgorithmException ex) {
      LOG.error("Error accessing MD5 algorithm file: {}", fileAbsolutePath, ex);
    }
    return null;
  }

  public int uploadFilewithPreSignedUrl() {
    HttpURLConnection connection;
    int noofBytesRead = 0;
    FileInputStream fileInputStream = null;
    try {
      final File file = new File(fileHashKey);
      LOG.debug("File size: " + file.length());
      fileInputStream = new FileInputStream(file);
      byte[] fileBytesbuffer = new byte[FILE_CHUNK_SIZE];
      connection = (HttpURLConnection) preSignedUrl.openConnection();
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.setUseCaches(false);
      final OutputStream out = connection.getOutputStream();
      final BufferedOutputStream bout = new BufferedOutputStream(out);
      while ((noofBytesRead = fileInputStream.read(fileBytesbuffer)) != -1) {
        bout.write(fileBytesbuffer, 0, noofBytesRead);
        bout.flush();
      }
      bout.close();
      return uploadStatus = connection.getResponseCode();

    } catch (ProtocolException ex) {
      LOG.error("Error making put request to S3 presignedUrl: {} fileName: {}", preSignedUrl, getFileAbsolutePath());
      throw new IngestRuntimeException("Error making put request to S3 fileName:" + getFileAbsolutePath(), ex);

    } catch (IOException ex) {
      LOG.error("Error reading File to OutputStream: {} fileName: {}", preSignedUrl, getFileAbsolutePath());
      throw new IngestRuntimeException("Error IO fileName" + getFileAbsolutePath(), ex);
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (IOException ex) {
          LOG.error("Error closing fileinputStream", ex);
        }
      }
    }
  }

  @Override public void run() {
    if (preSignedUrl != null) {
      uploadFilewithPreSignedUrl();
    }
  }

  private void cacheFile() {
    try {
      Files.copy(Paths.get(fileAbsolutePath), Paths.get(fileHashKey), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ex) {
      throw new IngestRuntimeException("IOError copying file to cache", ex);
    }
  }

  private boolean clean() {
    try {
      final File cachedfile = new File(fileHashKey);
      if (cachedfile.exists() && cachedfile.delete()) {
        return true;
      } else {
        return false;
      }
    } catch (SecurityException ex) {
      LOG.error("Error while deleting the file:{}", fileAbsolutePath, ex);
      throw new IngestRuntimeException("Error while trying to delete the file", ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3File.class);


}
