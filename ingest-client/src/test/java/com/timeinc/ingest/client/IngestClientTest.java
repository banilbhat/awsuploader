package com.openthoughts.ingest.client;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.openthoughts.ape.dev.common.config.DynamicPropertyFactory;
import com.openthoughts.ape.dev.common.config.DynamicVariableLookup;
import com.openthoughts.ingest.client.file.S3File;
import com.openthoughts.ingest.client.upload.LocalCache;
import com.openthoughts.ingest.client.upload.MultiPartS3Upload;
import com.openthoughts.ingest.client.upload.PreSignedUrlResponse;
import com.openthoughts.ingest.client.upload.PreSignedUrlS3Upload;

import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;


public class IngestClientTest {

  private File hotFolder;


  @Before
  public void setUp() throws Exception {
    // create the hot folder
    hotFolder = new File("src/test/resources");
  }

  @Test
  public void presignedUrlUpload() throws MalformedURLException {
    String folderPath = hotFolder.getAbsolutePath();
    PreSignedUrlS3Upload algo = new PreSignedUrlS3Upload(folderPath);
    PreSignedUrlS3Upload spyalgo = spy(algo);
    doReturn(new PreSignedUrlResponse(new URL("http://sampleamazons3url"), 200)).when(spyalgo)
        .generatePresignedUrl(anyObject());
    doReturn(true).when(spyalgo).uploadFile(anyObject(), anyBoolean());
    doNothing().when(spyalgo).updateUploadInfo(anyObject());
    boolean success = spyalgo.uploadFolder(false);
    assertEquals(spyalgo.getFileCount(), 3);
    assertThat(success, is(true));
  }

  @Test
  public void multiPartUpload() {
    MultiPartS3Upload algo = new MultiPartS3Upload(hotFolder.getAbsolutePath());
    MultiPartS3Upload spyalgo = spy(algo);
    AmazonS3Client s3Client = mock(AmazonS3Client.class);
    doReturn(s3Client).when(spyalgo)
        .buildCredentials();
    doNothing().when(spyalgo).updateUploadInfo(anyObject());
    doReturn(true).when(spyalgo).uploadFile(anyObject(), anyObject());
    boolean success = spyalgo.uploadFolder(false);
    assertEquals(spyalgo.getFileCount(), 3);
    assertThat(success, is(true));

  }


  @Test
  public void dynamicconfigurationlookup() {
    final DynamicVariableLookup dynamicVariableLookup =
        new DynamicVariableLookup(
            DynamicPropertyFactory.propertyFactory("dam-ingest-client-test"), true);
    final String propMessage = dynamicVariableLookup.lookup("DYNAMIC_PROP_MESSAGE");
    assertEquals(propMessage, "Hello from dynamic property message");
  }

  @Test
  public void fileHashKey() {
    final S3File s3File = new S3File(hotFolder.getAbsolutePath() + "/testImage.jpg");
    final String fileHashKey = s3File.getFileHashKey();
    assertEquals("e2fd893c58b5d6eb821829b8", fileHashKey);
  }

}
