package com.openthoughts.ingest.client.upload;


import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
public class LocalCache {

  private HashMap<String, String> uploadStatusMap;
  private Set<String> retrySet;
  private Set<String> retryRemoveSet;
  private long lastParseTimeStamp;
  private long currentParseTimeStamp;
  private File uploadStatusFile;
  private final String LAST_PARSE_TIME_STAMP = "lastParseTimestamp";
  private final String LAST_UPLOAD_FOLDER = "lastUploadFolderPath";

  private LocalCache() {
    initialize();
  }


  public long getLastParseTimeStamp() {
    return lastParseTimeStamp;
  }

  public void put(final String key, final String val, final String filePath) {
    uploadStatusMap.put(key, val);
    if (val.contains("0") && !retrySet.contains(filePath)) {
      retrySet.add(filePath);
    } else if (!val.contains("0") && retrySet.contains(filePath)) {
      retrySet.remove(filePath);
    }
  }

  public String get(final String key) {
    return uploadStatusMap.get(key);
  }

  public void remove(final String key) {
    uploadStatusMap.remove(key);
  }

  public void removeFromRetryCache(final String filePath) {
    retryRemoveSet.add(filePath);
  }

  public void addToRetrySet(final String filePath) {
    if (!retrySet.contains(filePath)) {
      retrySet.add(filePath);
    }
  }

  public void setCurrentParseTimeStamp(long currentParseTimeStamp) {
    this.currentParseTimeStamp = currentParseTimeStamp;
  }

  public boolean contains(final String key) {
    return uploadStatusMap.containsKey(key);
  }

  public boolean commit(final String updateFolderPath) {
    try {
      commitRetrySet();
      if (uploadStatusMap == null) {
        return true;
      }
      if (uploadStatusMap.isEmpty()) {
        if (uploadStatusFile.exists()) {
          return uploadStatusFile.delete();
        } else {
          return true;
        }
      }
      final ObjectMapper mapper = new ObjectMapper();
      uploadStatusMap.put(LAST_PARSE_TIME_STAMP, String.valueOf(currentParseTimeStamp));

      uploadStatusMap.put(LAST_UPLOAD_FOLDER, updateFolderPath);
      mapper.writeValue(uploadStatusFile, uploadStatusMap);
      return true;
    } catch (IOException ex) {
      LOG.error("Error writing to Filecache", ex);
      throw new IngestRuntimeException("IO Error deleting localcache file", ex);
    } catch (SecurityException ex) {
      LOG.error("Error deleting localcache file", ex);
      throw new IngestRuntimeException("Permission Error deleting localcache file", ex);
    }
  }

  private boolean commitRetrySet() {
    if (retrySet == null) {
      return true;
    }
    retrySet.removeAll(retryRemoveSet);
    if (retrySet.isEmpty()) {
      final File retrySetFile = new File("retrySet.tus");
      if (retrySetFile.exists()) {
        return retrySetFile.delete();
      } else {
        return true;
      }
    }
    try (
        final OutputStream file = new FileOutputStream("retrySet.tus");
        final OutputStream buffer = new BufferedOutputStream(file);
        final ObjectOutput output = new ObjectOutputStream(buffer);
    ) {
      output.writeObject(retrySet);
      return true;
    } catch (IOException ex) {
      LOG.error("Error commiting to RetrySet.", ex);
      throw new IngestRuntimeException("Error commiting to RetrySet", ex);
    }
  }

  public String getLastUploadFolderPath() {
    return uploadStatusMap.get(LAST_UPLOAD_FOLDER);
  }

  public Set<String> getRetrySet() {
    return retrySet;
  }


  private void initialize() {
    try {
      if (uploadStatusFile == null) {
        initializeRetrySet();
        currentParseTimeStamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        uploadStatusFile = new File("fileUploadStatus.tus");
        if (!uploadStatusFile.exists()) {
          uploadStatusMap = new HashMap<String, String>();
          lastParseTimeStamp = Long.MIN_VALUE;
          return;
        }
        final ObjectMapper mapper = new ObjectMapper();
        final TypeReference<HashMap<String, String>> typeRef
            = new TypeReference<HashMap<String, String>>() {
        };
        uploadStatusMap = mapper.readValue(uploadStatusFile, typeRef);
        lastParseTimeStamp = Long.parseLong(uploadStatusMap.get(LAST_PARSE_TIME_STAMP));
      }
    } catch (IOException ex) {
      LOG.error("IO Error initializing localcache", ex);
      throw new IngestRuntimeException("IO Error initializing localcache", ex);
    }
  }

  private void initializeRetrySet() {
    final File retrySetFile = new File("retrySet.tus");
    retryRemoveSet = new HashSet<String>();
    if (!retrySetFile.exists()) {
      retrySet = new HashSet<String>();
      return;
    }
    try (
        final InputStream file = new FileInputStream("retrySet.tus");
        final InputStream buffer = new BufferedInputStream(file);
        final ObjectInput input = new ObjectInputStream(buffer);
    ) {
      //deserialize the List
      retrySet = (Set<String>) input.readObject();
    } catch (FileNotFoundException ex) {
      throw new IngestRuntimeException("Error initializing retySet in LocalCache", ex);
    } catch (IOException ex) {
      throw new IngestRuntimeException("Error initializing retySet in LocalCache", ex);
    } catch (ClassNotFoundException ex) {
      throw new IngestRuntimeException("Error initializing retySet in LocalCache", ex);
    }
  }

  public static LocalCache getInstance() {
    if (localCache == null) {
      localCache = new LocalCache();
    }
    return localCache;
  }

  private static LocalCache localCache;
  private static final Logger LOG = LoggerFactory.getLogger(LocalCache.class);

}
