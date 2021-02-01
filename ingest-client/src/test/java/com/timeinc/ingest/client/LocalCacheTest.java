package com.openthoughts.ingest.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import com.openthoughts.ingest.client.upload.LocalCache;

import org.junit.Test;

import java.io.File;
public class LocalCacheTest {

  @Test
  public void localCacheputget() {
    LocalCache.getInstance().put("e2fd893c58b5d6eb821829b", "1,1", "/samplefolderPath");
    final String updateFolder = LocalCache.getInstance().getLastUploadFolderPath();
    final String val = LocalCache.getInstance().get("e2fd893c58b5d6eb821829b");
    assertEquals(val, "1,1");
  }

  @Test
  public void localCacheRetrySet() {
    LocalCache.getInstance().addToRetrySet("/samplefolderPath");
    final boolean actual = LocalCache.getInstance().getRetrySet().contains("/samplefolderPath");
    assertThat(actual, is(true));
  }

  @Test
  public void localCacheFile() {
    final LocalCache localCache = LocalCache.getInstance();
    final File f = new File("fileUploadStatus.tus");
    String lastUploadFolder = localCache.getLastUploadFolderPath();
    if (f.exists()) {
      assertNotNull(lastUploadFolder);
      final long timeStamp = localCache.getLastParseTimeStamp();
      localCache.setCurrentParseTimeStamp(timeStamp);
    } else {
      lastUploadFolder = "/sampleTestFolder";
    }
    localCache.commit(lastUploadFolder);
    final boolean fileExists = f.exists();
    assertThat(fileExists, is(true));
  }
}
